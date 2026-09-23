"""Discord presentation transport; authentication/durable claims belong to service.

Never register these functions as model tools. The caller must validate the
actual interaction and claim its capture-bound operation before invoking I/O.
"""
from __future__ import annotations

import asyncio
import io
from typing import Any

import aiohttp
from discord.http import json_or_text
from discord.webhook.async_ import AsyncWebhookAdapter, async_context


class SingleAttemptWebhookAdapter(AsyncWebhookAdapter):
    """Keep native payload serialization, but never retry evidence transmission."""
    async def request(self, route, session, *, payload=None, multipart=None, files=None,
                      proxy=None, proxy_auth=None, reason=None, auth_token=None, params=None):
        if route.method != 'POST' or reason is not None or auth_token is not None:
            raise RuntimeError('unexpected private playback transport')
        form = aiohttp.FormData(quote_fields=False)
        for field in multipart or []:
            form.add_field(**field)
        if not multipart or payload is not None:
            raise RuntimeError('private playback requires one multipart upload')
        try:
            async with session.request(route.method, route.url, data=form, params=params,
                                       proxy=proxy, proxy_auth=proxy_auth) as response:
                if not 200 <= response.status < 300:
                    raise RuntimeError('private playback outcome unavailable; no retry')
                return await json_or_text(response)
        except (OSError, aiohttp.ClientError, ValueError):
            raise RuntimeError('private playback outcome unavailable; no retry') from None


WARNING = ('Approve & run enables normal Hermes tools for this exact transcript. '
                          'Sensitive actions still require separate confirmation. '
                          'Get audio is private playback only, not analysis.')

CONVERSATION_LABELS = {
    'unbound': 'Conversation not created',
    'creating': 'Conversation creation in progress',
    'rate_wait': 'Conversation deferred for Discord cooldown',
    'ready': 'Conversation ready',
    'failed': 'Conversation creation failed — manual recovery required',
    'uncertain': 'Conversation creation uncertain — manual recovery required',
}
CREATION_STOPPED_LABEL = 'Ring thread creation stopped after global HTTP 429; manual recovery required'
WAITING_LABEL = 'Waiting for Discord — will continue automatically'
PREFLIGHT_INTERRUPTED_LABEL = 'Discord connection interrupted — retrying'
ROUTE_FAILURE_CODES = frozenset({'rejected', 'rate_limited', 'unavailable', 'mismatch',
    'timeout', 'cancelled', 'transport', 'storage', 'interrupted', 'expired', 'reset',
    'route_cooldown', 'global_cooldown', 'http_429_route', 'http_429_shared', 'http_429_global',
    'preflight_cancelled'})


def card_payload(*, capture_id: int, transcript: str, expires_at: int,
                 audio_expires_at: int, state: str, route_state: str | None = None,
                 route_failure_code: str | None = None,
                 retry_not_before: int | None = None,
                 creation_stopped: bool = False) -> dict[str, Any]:
    """Exact bounded transcript only in embeds; never attach evidence files."""
    import discord

    if (type(creation_stopped) is not bool
            or (route_state is not None and (type(route_state) is not str
                or route_state not in CONVERSATION_LABELS))
            or (route_failure_code is not None and (type(route_failure_code) is not str
                or route_failure_code not in ROUTE_FAILURE_CODES))
            or (route_state is None and (route_failure_code is not None or creation_stopped))):
        raise ValueError('invalid conversation status')
    embed = discord.Embed(title=f'Ring capture {capture_id}', description=transcript)
    labels = {'posting': 'Awaiting approval', 'pending': 'Awaiting approval',
              'approved_waiting': 'Queued', 'dispatching': 'Running',
              'turn_finished': 'Turn finished', 'uncertain': 'Uncertain — manual review required',
              'failed': 'Failed — manual review required',
              'expired': 'Expired', 'reset': 'Cancelled by reset', 'rejected': 'Rejected'}
    waiting = (state == 'approved_waiting' and route_state in ('rate_wait', 'ready')
               and retry_not_before is not None)
    embed.add_field(name='State', value=WAITING_LABEL if waiting else labels[state], inline=False)
    embed.add_field(name='Approval expires', value=f'<t:{expires_at // 1000}:R>')
    embed.add_field(name='Private audio available until', value=f'<t:{audio_expires_at // 1000}:R>')
    if route_state is not None:
        # Projection only: neither readiness nor this process-local latch grants authority.
        embed.add_field(name='Conversation', value=(CREATION_STOPPED_LABEL if creation_stopped
                        else PREFLIGHT_INTERRUPTED_LABEL if
                        route_state == 'rate_wait' and route_failure_code == 'preflight_cancelled'
                        else CONVERSATION_LABELS[route_state]), inline=False)
    if (waiting and route_failure_code != 'global_cooldown'
            and 0 < retry_not_before // 1000 < expires_at // 1000):
        embed.add_field(name='Next attempt', value=f'<t:{retry_not_before // 1000}:R>')
    embed.set_footer(text=WARNING)
    return {'embeds': [embed], 'allowed_mentions': discord.AllowedMentions.none()}


async def begin_private_response(interaction: Any) -> Any:
    """Own the initial ack; public deferral can override a later ephemeral flag."""
    if interaction.response.is_done():
        raise RuntimeError('private interaction was already acknowledged')
    async with asyncio.timeout(2):
        return await interaction.response.defer(ephemeral=True, thinking=True)


async def send_private_audio(interaction: Any, *, capture_id: int, audio: bytes,
                             receipt: Any) -> Any:
    """Send one already-authorized recording privately; never retry on uncertainty."""
    import discord

    if (not isinstance(receipt, discord.InteractionCallbackResponse)
            or receipt.id != interaction.id or not receipt.is_ephemeral()):
        raise RuntimeError('private acknowledgement not proven for this interaction')
    adapter = async_context.get()
    if not isinstance(adapter, AsyncWebhookAdapter):
        raise RuntimeError('unsupported private playback transport')
    transport_token = async_context.set(adapter if isinstance(adapter, SingleAttemptWebhookAdapter)
                                        else SingleAttemptWebhookAdapter())
    buffer = io.BytesIO(audio)
    recording = discord.File(buffer, filename=f'capture-{capture_id}.m4a')
    try:
        async with asyncio.timeout(15):
            return await interaction.followup.send(
                content=f'Capture {capture_id} — private playback only; not an audio-analysis request.',
                file=recording,
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
                wait=True,
            )
    finally:
        recording.close()
        buffer.close()
        async_context.reset(transport_token)
