"""Networkless producer and optional installed-Hermes envelope regressions.

PYTHONPATH=package python -m unittest discover -s tests -p test_native_event_envelope.py -v
Set PEBBLE_REQUIRE_NATIVE_ENVELOPE=1 in the Hermes image to require its real
MessageEvent, SessionSource and _event_clause gate instead of local stand-ins.
No adapter admission, persistent state, provider or Discord API is invoked.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
import os
import sys
from types import ModuleType, SimpleNamespace as NS
import unittest
from unittest.mock import patch

import discord

from pebble_bridge.hermes_port import HermesPort

try:
    import gateway
except ModuleNotFoundError as exc:
    if exc.name != 'gateway':
        raise
    NATIVE_ENVELOPE = False
else:
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import Platform, SessionSource
    from gateway.session_envelope import _event_clause, _validate_native
    from hermes_state_runtime import RuntimeStoreError
    NATIVE_ENVELOPE = True


GUILD, PARENT, CARD, ACTOR, BOT = ('323456789012345678', '223456789012345678',
    '423456789012345678', '123456789012345678', '523456789012345678')


if not NATIVE_ENVELOPE:
    class MessageType(Enum):
        TEXT = 'text'

    @dataclass
    class MessageEvent:
        text: str
        message_type: MessageType
        source: object
        message_id: str = None
        allow_gateway_control: bool = True
        internal: bool = False
        metadata: dict = field(default_factory=dict)


@contextmanager
def event_types():
    if NATIVE_ENVELOPE:
        yield
        return
    base = ModuleType('gateway.platforms.base')
    base.MessageEvent, base.MessageType = MessageEvent, MessageType
    # Only the local producer tests need a stand-in for the optional Hermes
    # dependency. The target-image integration tests require its real modules.
    with patch.dict(sys.modules, {'gateway': ModuleType('gateway'),
            'gateway.platforms': ModuleType('gateway.platforms'),
            'gateway.platforms.base': base}):
        yield


class Adapter:
    """Synthetic signature-compatible producer; never a live Hermes adapter."""
    def __init__(self, metadata=None):
        self._active_sessions, self._session_tasks = {}, {}
        self.metadata = {} if metadata is None else metadata

    def _build_slash_event(self, interaction, text):
        return MessageEvent(text=text, message_type=MessageType.TEXT,
            source=self.build_source(chat_id=CARD, chat_type='thread', user_id=ACTOR,
                thread_id=CARD, parent_chat_id=PARENT), metadata=self.metadata)

    def _session_key_profile(self, source):
        return 'default'

    async def handle_message(self, event):
        raise AssertionError('admission must not run in this fixture')

    def build_source(self, chat_id, chat_name=None, chat_type='dm', user_id=None,
                     user_name=None, thread_id=None, chat_topic=None, user_id_alt=None,
                     chat_id_alt=None, is_bot=False, scope_id=None, guild_id=None,
                     parent_chat_id=None, message_id=None, role_authorized=False,
                     auto_thread_created=False, auto_thread_initial_name=None):
        fields = dict(chat_id=chat_id, chat_name=chat_name, chat_type=chat_type,
            user_id=user_id, user_name=user_name, thread_id=thread_id, chat_topic=chat_topic,
            user_id_alt=user_id_alt, chat_id_alt=chat_id_alt, is_bot=is_bot,
            scope_id=scope_id, guild_id=guild_id, parent_chat_id=parent_chat_id,
            message_id=message_id, role_authorized=role_authorized,
            auto_thread_created=auto_thread_created, auto_thread_initial_name=auto_thread_initial_name)
        return (SessionSource(platform=Platform.DISCORD, **fields) if NATIVE_ENVELOPE
                else NS(platform=NS(value='discord'), **fields))

    def _format_thread_chat_name(self, thread):
        return thread.name


def thread_fixture():
    client = discord.Client(intents=discord.Intents.none())
    state = client._connection
    guild = state._add_guild_from_data(dict(id=GUILD, name='synthetic'))
    return discord.Thread(guild=guild, state=state, data=dict(
        id=CARD, parent_id=PARENT, owner_id=BOT, name='synthetic', type=11,
        message_count=0, member_count=1, thread_metadata=dict(
            archived=False, locked=False, auto_archive_duration=1440,
            archive_timestamp='2026-01-01T00:00:00+00:00')))


def thread_event(thread=None):
    with event_types():
        return HermesPort(Adapter()).thread_event(thread_fixture() if thread is None else thread,
            'Synthetic capture for envelope validation.', capture_id=42,
            card_message_id=CARD, guild_id=GUILD, parent_channel_id=PARENT, approver_id=ACTOR)


class ValidationRunner:
    """Fixed synthetic adapter identity and exact actor/destination allowlist."""
    config = NS(multiplex_profiles=False)

    def __init__(self):
        self.adapter = object()

    def _adapter_profile_for_source(self, source):
        return None

    def _adapter_flag(self, platform, flag, profile):
        return False

    def _is_user_authorized_for_source(self, source, *, allow_adapter_delegation):
        return (source.platform is Platform.DISCORD and source.user_id == ACTOR
                and source.scope_id == GUILD and source.guild_id == GUILD
                and source.parent_chat_id == PARENT and source.chat_id == CARD
                and source.thread_id == CARD and source.chat_type == 'thread'
                and source.is_bot is False and allow_adapter_delegation is False)

    def _adapter_for_source(self, source):
        return self.adapter if source.platform is Platform.DISCORD else None


class NativeEventProducerTests(unittest.TestCase):
    def test_thread_event_leaves_native_metadata_empty(self):
        event = thread_event()
        self.assertEqual(event.metadata, {})
        self.assertFalse(event.allow_gateway_control)
        self.assertFalse(event.internal)
        self.assertFalse(event.source.is_bot)
        self.assertEqual((event.source.user_id, event.source.scope_id, event.source.parent_chat_id,
                          event.source.chat_id, event.source.thread_id, event.message_id),
                         (ACTOR, GUILD, PARENT, CARD, CARD, CARD))

    def test_interaction_event_does_not_add_plugin_metadata(self):
        event = HermesPort(Adapter()).event(NS(guild_id=int(GUILD)), 'Synthetic capture.', capture_id=42)
        self.assertEqual(event.metadata, {})
        self.assertFalse(event.allow_gateway_control)
        self.assertEqual(event.source.scope_id, GUILD)

    def test_interaction_event_preserves_existing_metadata_for_native_validation(self):
        original = {'forged_native_authority': True}
        event = HermesPort(Adapter(original)).event(
            NS(guild_id=int(GUILD)), 'Synthetic capture.', capture_id=42)
        self.assertEqual(event.metadata, original)
        self.assertFalse(event.allow_gateway_control)

    def test_thread_event_still_refuses_mismatched_destination(self):
        thread = thread_fixture()
        thread.parent_id = int(PARENT) + 1
        with self.assertRaisesRegex(ValueError, 'invalid native thread event'):
            thread_event(thread)

    def test_thread_event_still_refuses_archived_destination(self):
        thread = thread_fixture()
        thread.archived = True
        with self.assertRaisesRegex(ValueError, 'invalid native thread event'):
            thread_event(thread)

    def test_target_image_can_require_real_native_envelope(self):
        if os.environ.get('PEBBLE_REQUIRE_NATIVE_ENVELOPE') == '1':
            self.assertTrue(NATIVE_ENVELOPE, 'the target-image gate requires installed Hermes modules')


@unittest.skipUnless(NATIVE_ENVELOPE, 'installed Hermes envelope unavailable in standalone plugin environment')
class InstalledHermesEnvelopeTests(unittest.TestCase):
    def test_real_thread_event_passes_native_event_clause(self):
        event = thread_event()
        self.assertIsInstance(event, MessageEvent)
        self.assertIsInstance(event.source, SessionSource)
        runner = NS(config=NS(multiplex_profiles=False))
        self.assertIsNone(_event_clause(runner, event, provenance=None, fresh_roles=False))

    def test_real_thread_event_passes_native_validation_and_source_roundtrip(self):
        event = thread_event()
        encoded = _validate_native(ValidationRunner(), event)
        restored = SessionSource.from_dict(encoded)
        self.assertEqual((restored.user_id, restored.scope_id, restored.guild_id,
                          restored.parent_chat_id, restored.chat_id, restored.thread_id),
                         (ACTOR, GUILD, GUILD, PARENT, CARD, CARD))
        self.assertIs(restored.platform, Platform.DISCORD)

    def test_forged_metadata_remains_rejected(self):
        event = thread_event()
        runner = NS(config=NS(multiplex_profiles=False))
        for metadata in ({'ring_capture_id': 42}, {'forged_native_authority': True},
                         {'whatsapp_from_owner': True}):
            with self.subTest(metadata=metadata):
                event.metadata = metadata
                self.assertEqual(_event_clause(runner, event, provenance=None, fresh_roles=False), 'metadata')
                with self.assertRaises(RuntimeStoreError) as error:
                    _validate_native(ValidationRunner(), event)
                self.assertEqual((error.exception.reason, error.exception.field), ('invalid_params', 'metadata'))

    def test_unauthorized_actor_remains_rejected(self):
        event = thread_event()
        event.metadata, event.source.user_id = {}, str(int(ACTOR) + 1)
        with self.assertRaises(RuntimeStoreError) as error:
            _validate_native(ValidationRunner(), event)
        self.assertEqual((error.exception.reason, error.exception.field),
                         ('permission_denied', 'user_not_authorized'))

    def test_forged_role_authorization_remains_rejected(self):
        event = thread_event()
        event.metadata, event.source.role_authorized = {}, True
        with self.assertRaises(RuntimeStoreError) as error:
            _validate_native(ValidationRunner(), event)
        self.assertEqual((error.exception.reason, error.exception.field),
                         ('invalid_params', 'role_authorized_without_fresh_roles'))

    def test_internal_event_remains_rejected(self):
        event = thread_event()
        event.metadata, event.internal = {}, True
        runner = NS(config=NS(multiplex_profiles=False))
        self.assertEqual(_event_clause(runner, event, provenance=None, fresh_roles=False), 'internal')
        with self.assertRaises(RuntimeStoreError) as error:
            _validate_native(ValidationRunner(), event)
        self.assertEqual((error.exception.reason, error.exception.field), ('invalid_params', 'internal'))


if __name__ == '__main__':
    unittest.main()
