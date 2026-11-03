"""Exact SDK pairs; stdlib runner: PYTHONPATH=package python -m unittest discover
-s tests -p test_native_sdk_compatibility.py. No sockets or Discord delivery.

Response fixtures replace ClientSession._request; disconnect, timeout and
cancellation tests replace only connector.connect. Real request contexts,
response JSON decoding, Discord caches and rate-limit reservations remain in use.
"""
import asyncio
import json
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

import aiohttp
import discord
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from pebble_bridge.service import RingService
from pebble_bridge.thread_port import ClosedTransport, CreationEvidence, SafeCooldown, ThreadPort

GUILD, PARENT, CARD, BOT = ('323456789012345678', '223456789012345678',
                            '423456789012345678', '523456789012345678')
USER = dict(id=BOT, username='synthetic-bot', discriminator='0000', avatar=None, bot=True)
CARD_DATA = dict(id=CARD, channel_id=PARENT, author=USER, content='synthetic',
                 timestamp='2026-01-01T00:00:00+00:00', edited_timestamp=None,
                 tts=False, mention_everyone=False, mentions=[], mention_roles=[],
                 attachments=[], embeds=[], pinned=False, type=0)
THREAD_DATA = dict(id=CARD, guild_id=GUILD, parent_id=PARENT, owner_id=BOT,
                   name='synthetic', type=11, message_count=0, member_count=1,
                   thread_metadata=dict(archived=False, auto_archive_duration=1440,
                       archive_timestamp='2026-01-01T00:00:00+00:00', locked=False))


class Adapter:
    _active_sessions = {}
    _session_tasks = {}

    def _build_slash_event(self, interaction, text):
        raise AssertionError('no dispatch')

    def _session_key_profile(self, source):
        return 'default'

    async def handle_message(self, event):
        raise AssertionError('no dispatch')


class StartupCompatibilityTests(unittest.TestCase):
    def test_unsupported_sdk_rejected_before_service_workers(self):
        for sdk, value in [(aiohttp, '3.14.4'), (aiohttp, '3.14.6'),
                           (aiohttp, '4.0.0'), (discord, '2.7.2')]:
            with self.subTest(sdk=sdk.__name__, version=value), patch.object(sdk, '__version__', value):
                with self.assertRaisesRegex(RuntimeError, 'unverified.*SDK'):
                    RingService(NS(), Adapter(), data_dir=None, scope={}, audio_retention_ms=3600000)

    def test_installed_supported_pair_can_construct_service(self):
        self.assertIn((discord.__version__, aiohttp.__version__),
                      {('2.7.1', '3.14.3'), ('2.7.1', '3.14.5')})
        service = RingService(NS(), Adapter(), data_dir=None, scope={}, audio_retention_ms=3600000)
        self.assertFalse(service.start_attempted)
        self.assertEqual((service.task, service.dispatch_task, service.ui_task, service.health_task),
                         (None, None, None, None))


class NativeTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = discord.Client(intents=discord.Intents.none())
        state = self.client._connection
        state.user = discord.ClientUser(state=state, data=USER)
        state._add_guild_from_data(dict(id=GUILD, name='synthetic',
            channels=[dict(id=PARENT, name='ring', type=0, position=0)],
            members=[dict(user=USER, roles=[], joined_at='2026-01-01T00:00:00+00:00',
                          deaf=False, mute=False, flags=0)]))
        self.session = aiohttp.ClientSession()
        self.addAsyncCleanup(self.session.close)
        self.client.http._HTTPClient__session = self.session
        self.client.http.token = 'synthetic-not-a-credential'
        self.client.http._global_over = asyncio.Event()
        self.client.http._global_over.set()

    def wire(self, replies):
        """Replace only the unavailable wire; return actual aiohttp responses."""
        calls, responses = [], []

        async def request(method, url, **kwargs):
            calls.append((method, url, kwargs))
            status, data, extra = replies[len(calls) - 1]
            target = URL(url)
            headers = CIMultiDictProxy(CIMultiDict({
                'Content-Type': 'application/json', 'X-Ratelimit-Limit': '5',
                'X-Ratelimit-Remaining': '4', 'X-Ratelimit-Reset-After': '0', **extra}))
            response = aiohttp.ClientResponse(method, target, writer=None, continue100=None,
                timer=aiohttp.helpers.TimerNoop(),
                request_info=aiohttp.RequestInfo(target, method, CIMultiDictProxy(CIMultiDict()), target),
                traces=[], loop=asyncio.get_running_loop(), session=self.session,
                stream_writer=NS(output_size=0))
            response.status, response._headers = status, headers
            response._body = data if isinstance(data, bytes) else json.dumps(data).encode()
            responses.append(response)
            return response

        replacement = patch.object(self.session, '_request', request)
        replacement.start()
        self.addCleanup(replacement.stop)
        return calls, responses

    def block_connector(self):
        requests = []

        async def connect(request, *, traces, timeout):
            requests.append(request)
            await asyncio.Event().wait()

        replacement = patch.object(self.session.connector, 'connect', connect)
        replacement.start()
        self.addCleanup(replacement.stop)
        return requests

    def assert_reservations_released(self):
        self.assertTrue(self.client.http._buckets)
        self.assertTrue(all(bucket.outgoing == 0 for bucket in self.client.http._buckets.values()))

    async def test_installed_sdk_creation_and_readback_use_native_transport(self):
        calls, responses = self.wire([(200, CARD_DATA, {}), (201, THREAD_DATA, {}),
                                     (200, CARD_DATA, {}), (200, THREAD_DATA, {})])
        port, evidence, approvals = ThreadPort(self.client), CreationEvidence(), []
        message, parent, thread, member = await port.readback(GUILD, PARENT, CARD)
        self.assertIsInstance(message, discord.Message)
        self.assertEqual((str(message.id), str(parent.id), str(member.id), thread),
                         (CARD, PARENT, BOT, None))
        await port.create_once(PARENT, CARD, name='synthetic', evidence=evidence,
                               authorize=lambda: approvals.append(True) or True)
        message, parent, thread, member = await port.readback(GUILD, PARENT, CARD, thread_id=CARD)
        self.assertIsInstance(thread, discord.Thread)
        self.assertEqual(str(thread.id), CARD)
        self.assertEqual((evidence.known_thread_id, approvals), (CARD, [True]))
        self.assertEqual([method for method, _, _ in calls], ['GET', 'POST', 'GET', 'GET'])
        self.assertTrue(calls[1][1].endswith(f'/channels/{PARENT}/messages/{CARD}/threads'))
        self.assertEqual(calls[1][2]['json'], {'name': 'synthetic', 'auto_archive_duration': 1440})
        self.assertTrue(all(args['allow_redirects'] is False for _, _, args in calls))
        self.assertTrue(all(response._released for response in responses))
        self.assert_reservations_released()

    async def test_create_invalid_json_stays_uncertain_without_retry(self):
        for data, headers in [(b'{', {}), (dict(id=CARD), {'Content-Type': 'text/plain'})]:
            with self.subTest(data=data, headers=headers):
                calls, responses = self.wire([(201, data, headers)])
                with self.assertRaises(ClosedTransport) as error:
                    await ThreadPort(self.client).create_once(PARENT, CARD, name='synthetic')
                self.assertEqual((error.exception.failure_code, error.exception.uncertain),
                                 ('invalid_response', True))
                self.assertEqual(len(calls), 1)
                self.assertTrue(responses[0]._released)
                self.assert_reservations_released()

    async def test_create_timeout_stays_uncertain_without_retry(self):
        calls = self.block_connector()
        with self.assertRaises(ClosedTransport) as error:
            await ThreadPort(self.client).create_once(PARENT, CARD, name='synthetic', timeout=0.01)
        self.assertEqual((error.exception.failure_code, error.exception.uncertain), ('timeout', True))
        self.assertEqual(len(calls), 1)
        self.assert_reservations_released()

    async def test_real_session_post_disconnect_is_not_retried(self):
        requests = []

        async def disconnect(request, *, traces, timeout):
            requests.append(request)
            raise aiohttp.ServerDisconnectedError('synthetic disconnect before socket access')

        with patch.object(self.session.connector, 'connect', disconnect):
            with self.assertRaises(ClosedTransport) as error:
                await ThreadPort(self.client).create_once(PARENT, CARD, name='synthetic')
        self.assertEqual((error.exception.failure_code, error.exception.uncertain),
                         ('transport_error', True))
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].method, 'POST')
        self.assertEqual(requests[0].headers['Content-Type'], 'application/json')
        self.assert_reservations_released()

    async def test_unsupported_runtime_pair_refuses_before_wire(self):
        calls, _ = self.wire([])
        with patch.object(aiohttp, '__version__', '3.14.6'):
            for operation in (ThreadPort(self.client).readback(GUILD, PARENT, CARD),
                              ThreadPort(self.client).create_once(PARENT, CARD, name='synthetic')):
                with self.assertRaises(ClosedTransport) as error:
                    await operation
                self.assertEqual((error.exception.failure_code, error.exception.uncertain),
                                 ('unsupported_native', False))
        self.assertEqual(calls, [])

    async def test_create_cancellation_is_preserved_without_retry(self):
        calls = self.block_connector()
        task = asyncio.create_task(ThreadPort(self.client).create_once(PARENT, CARD, name='synthetic'))
        async with asyncio.timeout(1):
            while not calls:
                if task.done():
                    await task
                await asyncio.sleep(0)
        task.cancel('synthetic cancellation')
        with self.assertRaisesRegex(asyncio.CancelledError, 'synthetic cancellation'):
            await task
        self.assertEqual(len(calls), 1)
        self.assert_reservations_released()

    async def test_global_429_retains_native_gate_without_retry(self):
        calls, responses = self.wire([(429, {'retry_after': 60, 'global': True},
            {'Via': 'synthetic', 'X-Ratelimit-Scope': 'global'})])
        port = ThreadPort(self.client)
        try:
            with self.assertRaises(SafeCooldown) as error:
                await port.create_once(PARENT, CARD, name='synthetic')
            self.assertEqual(error.exception.failure_code, 'http_429_global')
            self.assertFalse(error.exception.uncertain)
            self.assertEqual(len(calls), 1)
            self.assertTrue(responses[0]._released)
            self.assertFalse(self.client.http._global_over.is_set())
            self.assertIsNone(port.creation_failure)
            self.assert_reservations_released()
        finally:
            if port._global_release is not None:
                port._global_release.cancel()


if __name__ == '__main__':
    unittest.main()
