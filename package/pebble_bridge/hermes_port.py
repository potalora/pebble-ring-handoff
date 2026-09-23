"""Small, fail-closed compatibility boundary around native Hermes dispatch.

No client, model or credential is created here. The caller must authenticate and
claim the capture before dispatch; normal gateway authorization still applies.
"""
from __future__ import annotations

import asyncio
from typing import Any


def audio_identity() -> dict[str, str]:
    """Strict native ContextVars only; never trust ambient session env variables."""
    from gateway.session_context import _VAR_MAP

    names = {'actor_id': 'HERMES_SESSION_USER_ID', 'guild_id': 'HERMES_SESSION_SCOPE_ID',
             'chat_id': 'HERMES_SESSION_CHAT_ID', 'message_id': 'HERMES_SESSION_MESSAGE_ID',
             'profile': 'HERMES_SESSION_PROFILE'}
    if _VAR_MAP['HERMES_SESSION_PLATFORM'].get() != 'discord':
        raise PermissionError('no authenticated Discord turn')
    values = {key: _VAR_MAP[name].get() for key, name in names.items()}
    if any(not isinstance(value, str) for value in values.values()):
        raise PermissionError('no authenticated Discord turn')
    if any(not values[key] for key in ('actor_id', 'guild_id', 'chat_id', 'message_id')):
        raise PermissionError('no authenticated Discord turn')
    values['profile'] = values['profile'] or 'default'
    return values


class HermesPort:
    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter
        import inspect
        from importlib.metadata import version

        # These private seams are deliberately pinned, not a generic adapter API.
        # Runtime upgrades require the isolated behavioral gate to be rerun.
        if version('discord.py') != '2.7.1':
            raise RuntimeError('unverified Discord SDK version')
        expected = {'_build_slash_event': ('interaction', 'text'),
                    '_session_key_profile': ('source',), 'handle_message': ('event',)}
        for name, parameters in expected.items():
            method = getattr(adapter, name, None)
            if not callable(method):
                raise RuntimeError('incompatible Hermes dispatch interface')
            signature = inspect.signature(method)
            if (tuple(signature.parameters) != parameters
                    or any(p.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD
                           for p in signature.parameters.values())
                    or inspect.iscoroutinefunction(method) != (name == 'handle_message')):
                raise RuntimeError('incompatible Hermes dispatch signature')
        for name in ('_active_sessions', '_session_tasks'):
            if not isinstance(getattr(adapter, name, None), dict):
                raise RuntimeError('incompatible Hermes task tracking')

    def event(self, interaction: Any, text: str, *, capture_id: int) -> Any:
        event = self.adapter._build_slash_event(interaction, text)
        event.source.scope_id = str(interaction.guild_id)
        event.allow_gateway_control = False
        event.metadata = {**(event.metadata or {}), 'ring_capture_id': capture_id}
        return event

    def key(self, event: Any) -> str:
        from gateway.platforms.base import build_session_key

        extra = self.adapter.config.extra
        return build_session_key(
            event.source,
            group_sessions_per_user=extra.get('group_sessions_per_user', True),
            thread_sessions_per_user=extra.get('thread_sessions_per_user', False),
            profile=self.adapter._session_key_profile(event.source),
        )

    def idle(self, event: Any) -> bool:
        return self.key(event) not in self.adapter._active_sessions

    async def admit(self, event: Any, *, on_admitted=None) -> asyncio.Task:
        import inspect

        if on_admitted is not None and (
                not callable(on_admitted)
                or inspect.iscoroutinefunction(on_admitted)
                or inspect.isasyncgenfunction(on_admitted)
                or inspect.iscoroutinefunction(getattr(on_admitted, '__call__', None))
                or inspect.isasyncgenfunction(getattr(on_admitted, '__call__', None))):
            raise TypeError('native admission callback must be synchronous')
        key = self.key(event)
        if not self.idle(event):
            raise RuntimeError('normal Hermes session is busy')
        previous = self.adapter._session_tasks.get(key)
        await self.adapter.handle_message(event)
        task = self.adapter._session_tasks.get(key)
        if not isinstance(task, asyncio.Task) or task is previous or task is asyncio.current_task():
            raise RuntimeError('native Hermes did not create a new owner task')
        if on_admitted is not None:
            # Admission has occurred. Callback failure must not cancel or replay
            # the native owner, nor be mistaken for completed execution.
            result = on_admitted(task)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError('native admission callback returned an awaitable')
        return task

    async def dispatch(self, event: Any, *, on_admitted=None) -> None:
        task = await self.admit(event, on_admitted=on_admitted)
        await task

    def thread_preflight(self) -> None:
        """Explicit startup compatibility gate; no events are built or sent."""
        self._validate_thread_interface(mention_settings=True)
        try:
            compatible = (self.adapter._discord_require_mention() is False
                          and self.adapter._discord_thread_require_mention() is False)
        except Exception:
            raise RuntimeError('incompatible Hermes thread mention settings') from None
        if not compatible:
            raise RuntimeError('incompatible Hermes thread mention settings')

    def _validate_thread_interface(self, *, mention_settings: bool = False) -> None:
        import inspect

        expected = {
            'build_source': ('chat_id', 'chat_name', 'chat_type', 'user_id', 'user_name',
                             'thread_id', 'chat_topic', 'user_id_alt', 'chat_id_alt',
                             'is_bot', 'scope_id', 'guild_id', 'parent_chat_id',
                             'message_id', 'role_authorized', 'auto_thread_created',
                             'auto_thread_initial_name'),
            '_format_thread_chat_name': ('thread',),
        }
        if mention_settings:
            expected.update(_discord_require_mention=(), _discord_thread_require_mention=())
        for name, parameters in expected.items():
            method = getattr(self.adapter, name, None)
            try:
                signature = inspect.signature(method)
            except (TypeError, ValueError):
                raise RuntimeError('incompatible Hermes thread interface') from None
            if (not callable(method) or inspect.iscoroutinefunction(method)
                    or tuple(signature.parameters) != parameters
                    or any(p.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD
                           for p in signature.parameters.values())):
                raise RuntimeError('incompatible Hermes thread interface')

    def thread_event(self, thread: Any, text: str, *, capture_id: int,
                     card_message_id: str, guild_id: str, parent_channel_id: str,
                     approver_id: str) -> Any:
        """Construct conversational input; preceding caller gates grant authority."""
        import discord
        from gateway.platforms.base import MessageEvent, MessageType

        self._validate_thread_interface()
        ids = (card_message_id, guild_id, parent_channel_id, approver_id)
        if (any(type(value) is not str or not 17 <= len(value) <= 20
                or not value.isascii() or not value.isdecimal() or value[0] == '0'
                for value in ids)
                or type(capture_id) is not int or capture_id <= 0
                # Request transcripts originate at receiver.TRANSCRIPT_LIMIT (4000).
                or not isinstance(text, str) or len(text) > 4000 or not text.strip()):
            raise ValueError('invalid native thread event')
        if (type(thread) is not discord.Thread
                or thread.type is not discord.ChannelType.public_thread
                or thread.archived is not False or thread.locked is not False
                or thread.id != int(card_message_id)
                or thread.parent_id != int(parent_channel_id)
                or getattr(thread.guild, 'id', None) != int(guild_id)):
            raise ValueError('invalid native thread event')

        source = self.adapter.build_source(
            chat_id=str(thread.id), thread_id=str(thread.id),
            chat_type='thread', chat_name=self.adapter._format_thread_chat_name(thread),
            guild_id=guild_id, scope_id=guild_id, parent_chat_id=parent_channel_id,
            user_id=approver_id, user_name='Approved capture actor', is_bot=False,
            message_id=card_message_id,
        )
        return MessageEvent(text=text, message_type=MessageType.TEXT, source=source,
                            message_id=card_message_id, allow_gateway_control=False,
                            metadata={'ring_capture_id': capture_id})
