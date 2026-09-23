"""Single-card publisher; decision wiring is an explicit next development slice."""
import asyncio
import time
import hashlib
import secrets
import sqlite3
import math
from functools import partial
from types import SimpleNamespace

import discord

from .discord_ui import WARNING, card_payload, begin_private_response, send_private_audio
from .audio import retrieve_audio, purge_expired_audio
from .hermes_port import HermesPort
from .admission_guard import AdmissionGuard
from .worker_health import write_health
from .relay_store import RelayStore, Request
from .thread_port import ThreadPort, CreationEvidence, ClosedTransport, SafeCooldown
from .thread_validation import DestinationRejected, validate_parent_card, validate_thread

MAX_PARALLEL_CAPTURE_TURNS = 4


class RingService:
    def __init__(self, native, adapter, *, data_dir, scope, audio_retention_ms):
        self.native, self.port = native, HermesPort(adapter)
        self.thread_port = ThreadPort(native)
        self.data_dir, self.scope = data_dir, scope
        self.audio_retention_ms = audio_retention_ms
        self.start_attempted = False
        self.task = None
        self.dispatch_task = None
        self.ui_task = None
        self.rendered = {}
        self.next_cleanup = 0
        self.cleanup_at_ms = 0
        self.health_task = None
        self.known_thread_ids = set()
        self.notice_ids = {}
        self.pending_notices = {}
        self.stopping = False
        self.projection_failed = False
        self._dispatch_lock = asyncio.Lock()
        self._dispatch_permit = None
        self._dispatch_caller = None
        self._admitted = {}
        # Existing native probes observe the sole owner through these fields.
        # Authorization uses _admitted, including when owners overlap.
        self._admitted_event = self._admitted_owner = self._admitted_key = None
        self._dispatch_runs = {}
        self.guard = AdmissionGuard(adapter, is_ring_scope=self.is_ring_scope,
                                    allow=self.allow_event, notify=self.notify_denied,
                                    route=self.route_human_event)

    async def route_human_event(self, event):
        from .human_threads import route_human_event
        routed = await route_human_event(self, event)
        if (event.source.chat_id == self.scope['channel_id']
                and routed is not event and routed.source.chat_id != event.source.chat_id
                and self.authenticated(routed)):
            # The raw parent message was verified before thread creation. Bind
            # an explicit audio request to the thread where Hermes runs it.
            from .audio_tools import grant_from_event
            grant_from_event(self.data_dir, routed, scope=self.scope,
                             now_ms=int(time.time()*1000))
        return routed

    def is_ring_scope(self, event):
        source = event.source
        return (getattr(source.platform, 'value', None) == 'discord' and
                (source.chat_id in self.known_thread_ids or
                 getattr(source, 'thread_id', None) in self.known_thread_ids or
                 (source.scope_id == self.scope['guild_id'] and
                  (source.chat_id == self.scope['channel_id'] or
                   source.parent_chat_id == self.scope['channel_id']))))

    def authenticated(self, event):
        source = event.source
        return (getattr(source.platform, 'value', None) == 'discord'
                and source.user_id == self.scope['approver_id']
                and source.scope_id == self.scope['guild_id']
                and source.is_bot is False and event.internal is False
                and event.allow_gateway_control is True
                and ((source.chat_id == self.scope['channel_id']
                      and source.chat_type == 'group' and not source.thread_id)
                     or (source.chat_type == 'thread'
                         and source.chat_id == source.thread_id
                         and source.parent_chat_id == self.scope['channel_id'])))

    def _refresh_single_owner(self):
        if len(self._admitted) == 1:
            self._admitted_key, (self._admitted_event, self._admitted_owner) = next(
                iter(self._admitted.items()))
        else:
            self._admitted_event = self._admitted_owner = self._admitted_key = None

    async def dispatch_thread(self, capture_id, event, *, now_ms=None):
        """Exact durable claim and native admission; lifecycle outcome is caller-owned.

        Caller supplies the freshly validated native thread event. This method
        does not create/validate destinations or migrate the legacy worker.
        """
        async with self._dispatch_lock:
            if self.guard._closed or self.port.adapter.handle_message is not self.guard._wrapper:
                raise RuntimeError('Ring admission guard unavailable')
            source = event.source
            with self.store() as store:
                request = store.get(capture_id)
                route = store.get_route(capture_id)
                if (request is None or route is None or route.thread_id != request.message_id
                        or event.text != request.transcript or event.message_id != request.message_id
                        or event.internal is not False or event.allow_gateway_control is not False
                        or source.is_bot is not False or source.chat_type != 'thread'
                        or source.chat_id != route.thread_id or source.thread_id != route.thread_id
                        or getattr(source.platform, 'value', None) != 'discord'
                        or source.user_id != self.scope['approver_id']
                        or source.scope_id != self.scope['guild_id']
                        or source.parent_chat_id != self.scope['channel_id']
                        or not self.port.idle(event)):
                    raise RuntimeError('Ring exact dispatch unavailable')
                claimed = store.claim_thread_dispatch(capture_id, **self.scope,
                    now_ms=int(time.time()*1000) if now_ms is None else now_ms)
            if claimed is None:
                raise RuntimeError('Ring exact dispatch unavailable')
            self.known_thread_ids.add(source.chat_id)
            self._dispatch_permit = event
            self._dispatch_caller = asyncio.current_task()
            key = self.port.key(event)
            def admitted(owner):
                self._dispatch_permit = None
                self._admitted[key] = (event, owner)
                self._refresh_single_owner()
            try:
                owner = await self.port.admit(event, on_admitted=admitted)
            except BaseException:
                self._admitted.pop(key, None)
                self._refresh_single_owner()
                raise
            finally:
                self._dispatch_permit = self._dispatch_caller = None
        try:
            await owner
        finally:
            if self._admitted.get(key) == (event, owner):
                self._admitted.pop(key, None)
                self._refresh_single_owner()

    def allow_event(self, event):
        if (event is self._dispatch_permit
                and asyncio.current_task() is self._dispatch_caller):
            return True
        if any(event is permitted and asyncio.current_task() is owner
                and self.port.adapter._session_tasks.get(key) is owner
                for key, (permitted, owner) in self._admitted.items()):
            return True
        if not self.authenticated(event):
            return False
        if event.source.chat_id == self.scope['channel_id']:
            return False  # Parent input must be routed before native admission.
        with self.store() as store:
            request = store.thread_request(event.source.chat_id, **self.scope)
            if request is None:
                # An absent route is not proof of an ordinary human thread:
                # pending, expired, or incomplete capture cards retain identity.
                return store.card_request(event.source.chat_id, **self.scope) is None
            if event.get_command() in {'new', 'reset'}:
                store.reset_thread(event.source.chat_id, **self.scope,
                                   reset_message_id=event.message_id)
                return True
            if request.state == 'dispatching':
                key = self.port.key(event)
                admitted = self._admitted.get(key)
                owner = admitted[1] if admitted is not None else None
                return (isinstance(owner, asyncio.Task) and not owner.done()
                        and admitted[0] is not None
                        and key in self.port.adapter._active_sessions
                        and self.port.adapter._session_tasks.get(key) is owner)
            if request.state == 'turn_finished':
                return True
            marker = store.thread_reset_message(event.source.chat_id, **self.scope)
            message = event.message_id
            return (request.state in {'failed', 'uncertain', 'expired', 'reset'}
                    and marker is not None and isinstance(message, str)
                    and 17 <= len(message) <= 20 and message.isascii()
                    and message.isdecimal() and message[0] != '0'
                    and int(marker) < int(message) <= 2**64 - 1)

    def _reserve_notice(self, event):
        if self.stopping or self.guard._closed or not self.authenticated(event):
            return None
        key = (event.source.chat_id, event.message_id)
        if any(not isinstance(value, str) or not 17 <= len(value) <= 20
               or not value.isascii() or not value.isdecimal() or value[0] == '0'
               or int(value) > 2**64 - 1 for value in key):
            return None
        now = time.monotonic()
        self.notice_ids = {key: stamp for key, stamp in self.notice_ids.items()
                           if now - stamp < 30}
        self.pending_notices = {key: stamp for key, stamp in self.pending_notices.items()
                                if now - stamp < 30}
        if key in self.notice_ids or len(self.notice_ids) >= 256:
            return None
        self.notice_ids[key] = now  # Reserve before any potentially uncertain send.
        return key

    def offer_notice(self, event):
        """Synchronous ID-only offer; never retains human input or spawns work."""
        if self.ui_task is None or self.ui_task.done():
            return
        key = self._reserve_notice(event)
        if key is not None:
            self.pending_notices[key] = self.notice_ids[key]

    async def _send_notice(self, key):
        channel = self.native.get_channel(int(key[0]))
        if (not isinstance(channel, (discord.TextChannel, discord.Thread))
                or str(channel.id) != key[0]
                or str(channel.guild.id) != self.scope['guild_id']):
            return
        if key[0] == self.scope['channel_id']:
            if not isinstance(channel, discord.TextChannel):
                return
        elif (not isinstance(channel, discord.Thread)
              or str(channel.parent_id) != self.scope['channel_id']):
            return
        notice = ('Ring is not ready for this input or state is unavailable. '
                  'Wait, then resend; nothing was queued.')
        if key[0] != self.scope['channel_id']:
            try:
                with self.store() as store:
                    request = store.card_request(key[0], **self.scope)
                    route = store.get_route(request.capture_id) if request is not None else None
                    if (request is not None and route is not None
                            and request.state in {'failed', 'uncertain'}
                            and route.state in {'failed', 'uncertain'}):
                        notice = ('This Ring capture needs manual recovery and will not run again. '
                                  'Send /new in this thread, then resend your question; '
                                  'your message was not queued.')
            except (sqlite3.Error, OSError, RuntimeError, ValueError):
                pass  # A missing diagnostic must not suppress the generic refusal.
        async with asyncio.timeout(1):
            await channel.send(notice, allowed_mentions=discord.AllowedMentions.none())

    async def notify_denied(self, event):
        key = self._reserve_notice(event)
        if key is not None:
            await self._send_notice(key)

    async def drain_notices(self):
        # A single total deadline bounds this tick, including all native sends.
        try:
            async with asyncio.timeout(1):
                for key in list(self.pending_notices)[:8]:
                    # A concurrent offer can prune expired keys while send awaits.
                    stamp = self.pending_notices.pop(key, None)
                    if stamp is None or time.monotonic() - stamp >= 30 or self.stopping:
                        continue
                    try:
                        await self._send_notice(key)
                    except Exception:
                        pass  # No retry or raw exception logging.
        except TimeoutError:
            pass

    async def create_claimed_thread(self, request, *, name) -> str:
        """Persist creation syntax evidence, never readiness or approval.

        Caller must already have atomically claimed creation, freshly validated
        scope/approval/the exact original card, and registered the prospective
        ID with the guard. This is not a new approval authority; do not wire it
        into lifecycle callers until those prerequisites are implemented.

        No retries or remote work follows creation. Synchronous finally keeps
        matching evidence durable even when creation raises or is cancelled.
        Persistence/commit/close failure takes precedence over a remote error
        (including cancellation), retained as Python exception context. No
        cancellation is caught, replaced, uncancelled, or retried here when
        persistence succeeds. Lifecycle failure mapping belongs to the caller.
        """
        evidence = CreationEvidence()
        def authorize():
            now = int(time.time()*1000)
            with self.store() as store:
                global_due = store.thread_global_retry_not_before(**self.scope, now_ms=now)
                if global_due is not None:
                    raise SafeCooldown('global_cooldown', operation='create', scope='global',
                                       retry_after=(global_due-now)/1000)
                return store.authorize_thread_creation(request.capture_id, expected_request=request,
                                                        **self.scope, now_ms=now)
        try:
            await self.thread_port.create_once(request.channel_id, request.message_id,
                                               name=name, evidence=evidence, authorize=authorize)
        finally:
            if evidence.known_thread_id is not None:
                if evidence.known_thread_id != request.message_id:
                    raise RuntimeError('Ring thread creation identity mismatch')
                with self.store() as store:
                    store.remember_thread(request.capture_id, evidence.known_thread_id)
        if evidence.known_thread_id is None:
            raise RuntimeError('Ring thread creation evidence unavailable')
        return evidence.known_thread_id

    async def validate_claimed_thread(self, request: Request) -> bool:
        """Read back a known binding and commit readiness, not dispatch authority.

        Initial invalid durable evidence returns False without remote work.
        Transport, destination, cancellation and storage failures propagate.
        Later dispatch still requires fresh validation and an exact atomic claim.
        """
        with self.store() as store:
            current = store.get(request.capture_id)
            route = store.get_route(request.capture_id)
            if (current != request or request.state != 'approved_waiting'
                    or any(getattr(request, key) != self.scope[key]
                           for key in ('guild_id', 'channel_id', 'approver_id'))
                    or int(time.time()*1000) >= request.expires_at
                    or route is None or route.state != 'creating'
                    or route.attempt_started_at is None or route.thread_id is None
                    or route.thread_id != request.message_id
                    or store.thread_request(route.thread_id, **self.scope) != request):
                return False
            known_id = route.thread_id
        message, parent, thread, member = await self.thread_port.readback(
            request.guild_id, request.channel_id, request.message_id, thread_id=known_id)
        # The retained transport guarantees genuine current native dependencies
        # at return. No await separates pure validation from the exact store gate.
        user = self.native.user
        if user is None or not isinstance(member, discord.Member) or member.id != user.id:
            raise DestinationRejected('unavailable')
        validate_parent_card(message, request, scope=self.scope, bot_id=user.id)
        validate_thread(thread, request, scope=self.scope, bot_member=member)
        with self.store() as store:
            ready = store.ready_thread(request.capture_id, **self.scope,
                now_ms=int(time.time()*1000), expected_request=request)
        return ready

    def store(self):
        return RelayStore(self.data_dir, audio_retention_ms=self.audio_retention_ms)

    def view(self, request):
        view = discord.ui.View(timeout=None)
        for action, label in [('approve','Approve & run'),('reject','Reject'),('audio','Get audio')]:
            button = discord.ui.Button(label=label,
                custom_id=f'ring:v3:{request.capture_id}:{request.nonce}:{action}')
            button.disabled = (request.state not in {'posting', 'pending'} if action != 'audio'
                               else request.state == 'rejected' or
                               int(time.time()*1000) >= request.audio_expires_at)
            button.callback = (partial(self.playback, request.capture_id, request.nonce)
                               if action == 'audio' else
                               partial(self.decide, request.capture_id, action, request.nonce))
            view.add_item(button)
        with self.store() as store:
            route = store.get_route(request.capture_id)
            verified = (route is not None and route.state == 'ready'
                and route.thread_id is not None and route.thread_id == request.message_id
                and store.thread_request(route.thread_id, **self.scope) == request)
        if verified:
            view.add_item(discord.ui.Button(label='Open conversation',
                url=f'https://discord.com/channels/{request.guild_id}/{route.thread_id}'))
        return view

    async def playback(self, capture_id, nonce, interaction):
        receipt = await begin_private_response(interaction)
        try:
            message = interaction.message
            with self.store() as store:
                request = store.get(capture_id)
                if (request is None or interaction.user.bot
                        or interaction.data.get('custom_id') != f'ring:v3:{capture_id}:{nonce}:audio'
                        or message.author.id != self.native.user.id
                        or message.attachments or len(message.embeds) != 1
                        or (str(interaction.user.id), str(interaction.guild_id), str(interaction.channel_id))
                           != (self.scope['approver_id'], self.scope['guild_id'], self.scope['channel_id'])
                        or str(message.id) != request.message_id
                        or not secrets.compare_digest(nonce, request.nonce)
                        or message.embeds[0].description != request.transcript):
                    raise PermissionError('invalid playback request')
                identity = dict(actor_id=str(interaction.user.id), guild_id=str(interaction.guild_id),
                    chat_id=str(interaction.channel_id), message_id=str(interaction.id), profile='playback')
                store.grant_audio(capture_id, **identity, capture_channel_id=str(interaction.channel_id),
                                  kind='playback', now_ms=int(time.time()*1000))
                recording = retrieve_audio(store, capture_id, identity=identity,
                    kind='playback', now_ms=lambda: int(time.time()*1000))
            await send_private_audio(interaction, capture_id=capture_id, audio=recording.data,
                                     receipt=receipt)
        except (PermissionError, OSError, sqlite3.Error, RuntimeError):
            async with asyncio.timeout(15):
                await interaction.followup.send('Audio unavailable or already requested; nothing was run.',
                    ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    async def decide(self, capture_id, action, nonce, interaction):
        await begin_private_response(interaction)
        expected = f'ring:v3:{capture_id}:{nonce}:{action}'
        message = interaction.message
        valid = (interaction.data.get('custom_id') == expected
                 and message.author.id == self.native.user.id
                 and not message.attachments and len(message.embeds) == 1
                 and message.embeds[0].footer.text == WARNING)
        outcome = 'invalid'
        if valid:
            digest = hashlib.sha256((message.embeds[0].description or '').encode()).hexdigest()
            with self.store() as store:
                outcome = store.decide(capture_id=capture_id, action=action, nonce=nonce, digest=digest,
                    actor_id=str(interaction.user.id), guild_id=str(interaction.guild_id),
                    channel_id=str(interaction.channel_id), message_id=str(message.id),
                    now_ms=int(time.time()*1000))
        async with asyncio.timeout(15):
            await interaction.followup.send(outcome, ephemeral=True,
                                            allowed_mentions=discord.AllowedMentions.none())

    def synthetic_interaction(self):
        channel = self.native.get_channel(int(self.scope['channel_id']))
        return SimpleNamespace(channel=channel, channel_id=int(self.scope['channel_id']),
            guild_id=int(self.scope['guild_id']),
            user=SimpleNamespace(id=int(self.scope['approver_id']), display_name='Ring approver'))

    def record_route_failure(self, request, exc):
        if type(exc) is SafeCooldown:
            now = int(time.time()*1000)
            # A native global Event has no published deadline. Recheck locally
            # once a second; ThreadPort must still see the gate open before I/O.
            delay_ms = math.ceil(1000 * exc.retry_after) if exc.retry_after is not None else 1000
            with self.store() as store:
                store.defer_thread(request.capture_id, expected_request=request,
                    **self.scope, now_ms=now, retry_not_before=now + max(1, delay_ms),
                    failure_code=exc.failure_code)
            return
        if isinstance(exc, DestinationRejected):
            with self.store() as store:
                store.fail_thread(request.capture_id, failure_code=exc.failure_code, uncertain=False)
            return
        failure = exc.failure_code
        codes = {'http_429': 'rate_limited', 'global_cooldown': 'rate_limited',
                 'route_cooldown': 'rate_limited', 'timeout': 'timeout',
                 'unavailable': 'unavailable', 'invalid_response': 'mismatch',
                 'transport_error': 'transport', 'accounting_error': 'transport',
                 'bucket_collision': 'transport', 'invalid_input': 'transport',
                 'unsupported_native': 'transport', 'authority_revoked': 'rejected'}
        code = codes.get(failure)
        if (code is None and type(failure) is str and len(failure) == 8
                and failure.startswith('http_')
                and all('0' <= char <= '9' for char in failure[5:])
                and (100 <= int(failure[5:]) < 200 or 300 <= int(failure[5:]) < 600)):
            code = 'rejected'
        if code is None:
            raise exc
        with self.store() as store:
            store.fail_thread(request.capture_id, failure_code=code, uncertain=exc.uncertain)
        if failure in {'invalid_input', 'unsupported_native'}:
            raise exc  # A different capture cannot repair process compatibility.

    async def _dispatch_one(self, request):
        try:
            message, parent, thread, member = await self.thread_port.readback(
                request.guild_id, request.channel_id, request.message_id,
                thread_id=request.message_id)
            user = self.native.user
            if user is None or not isinstance(member, discord.Member) or member.id != user.id:
                raise DestinationRejected('unavailable')
            validate_parent_card(message, request, scope=self.scope, bot_id=user.id)
            validate_thread(thread, request, scope=self.scope, bot_member=member)
        except asyncio.CancelledError:
            with self.store() as store:
                store.fail_thread(request.capture_id, failure_code='cancelled', uncertain=False)
            raise
        except (ClosedTransport, DestinationRejected) as exc:
            self.record_route_failure(request, exc)
            return
        event = self.port.thread_event(thread, request.transcript,
            capture_id=request.capture_id, card_message_id=request.message_id,
            guild_id=request.guild_id, parent_channel_id=request.channel_id,
            approver_id=request.approver_id)
        if not self.port.idle(event):
            return
        try:
            await self.dispatch_thread(request.capture_id, event)
            with self.store() as store:
                store.finish(request.capture_id)
        except BaseException:
            with self.store() as store:
                store.uncertain(request.capture_id)
            raise

    async def dispatch(self):
        try:
            while True:
                for capture_id, run in list(self._dispatch_runs.items()):
                    if run.done():
                        del self._dispatch_runs[capture_id]
                        await run  # A failed turn still stops this worker for manual review.
                if self.native.is_ready() and len(self._dispatch_runs) < MAX_PARALLEL_CAPTURE_TURNS:
                    with self.store() as store:
                        candidates = store.thread_candidates(**self.scope, now_ms=int(time.time()*1000))
                    for request in candidates:
                        if len(self._dispatch_runs) >= MAX_PARALLEL_CAPTURE_TURNS:
                            break
                        if request.capture_id not in self._dispatch_runs:
                            self._dispatch_runs[request.capture_id] = asyncio.create_task(
                                self._dispatch_one(request), name=f'ring-capture-{request.capture_id}')
                await asyncio.sleep(0.1)
        finally:
            runs = list(self._dispatch_runs.values())
            for run in runs:
                run.cancel()
            await asyncio.gather(*runs, return_exceptions=True)
            self._dispatch_runs.clear()

    async def on_ready(self):
        # The factory may defer activation while an older adapter closes.
        getattr(self, 'ready_callback', self.start)()

    def start(self):
        # No awaits: startup and duplicate-event checks are one event-loop turn.
        if self.start_attempted:
            if self.projection_failed or self.stopping:
                raise RuntimeError('Ring UI projection stopped; manual review required')
            if any(task is None or task.done() for task in
                   (self.task, self.dispatch_task, self.ui_task)):
                raise RuntimeError('Ring worker stopped; manual review required')
            return
        self.start_attempted = True
        self.port.thread_preflight()
        with self.store() as store:
            store.recover(**self.scope, now_ms=int(time.time()*1000))
            after = 0
            while requests := store.cards(**self.scope, after=after):
                for request in requests:
                    route = store.get_route(request.capture_id)
                    if route is not None and route.attempt_started_at is not None:
                        self.known_thread_ids.add(request.message_id)
                        if route.thread_id is not None:
                            self.known_thread_ids.add(route.thread_id)
                    self.native.add_view(self.view(request), message_id=int(request.message_id))
                after = requests[-1].capture_id
        self.guard.install()
        self.task = asyncio.create_task(self.publish(), name='ring-successor-publisher')
        self.dispatch_task = asyncio.create_task(self.dispatch(), name='ring-successor-dispatcher')
        self.ui_task = asyncio.create_task(self.reconcile(), name='ring-successor-ui')
        self.health_task = asyncio.create_task(self.observe_health(), name='ring-health-observer')

    async def observe_health(self):
        while True:
            write_health(self.data_dir, self.scope, now_ms=int(time.time()*1000),
                workers_ok=not self.projection_failed and all(task is not None and not task.done() for task in
                               (self.task, self.dispatch_task, self.ui_task)),
                native_ready=self.native.is_ready(), cleanup_at_ms=self.cleanup_at_ms,
                creation_stopped=(self.thread_port.creation_failure is not None))
            await asyncio.sleep(5)

    async def reconcile(self):
        # UI projection has no path to execution or authority renewal.
        while True:
            await self.drain_notices()
            if not self.projection_failed:
                try:
                    if self.native.is_ready():
                        with self.store() as store:
                            store.expire_due(**self.scope, now_ms=int(time.time()*1000))
                        after = 0
                        while True:
                            with self.store() as store:
                                requests = store.cards(**self.scope, after=after)
                            if not requests:
                                break
                            for request in requests:
                                with self.store() as store:
                                    route = store.get_route(request.capture_id)
                                creation_stopped = self.thread_port.creation_failure is not None
                                signature = (request.state, int(time.time()*1000) >= request.audio_expires_at,
                                    route.state if route else None, route.failure_code if route else None,
                                    route.thread_id if route else None, route.retry_not_before if route else None,
                                    creation_stopped)
                                if self.rendered.get(request.capture_id) == signature:
                                    continue
                                with self.store() as store:
                                    global_wait = store.thread_global_retry_not_before(
                                        **self.scope, now_ms=int(time.time()*1000))
                                if global_wait is not None or not self.thread_port.presentation_ready(
                                        request.channel_id, card_id=request.message_id):
                                    continue
                                async with self.thread_port.presentation_slot(
                                        request.channel_id, card_id=request.message_id) as available:
                                    if not available:
                                        continue
                                    channel = self.native.get_channel(int(self.scope['channel_id']))
                                    # One total deadline covers fetch, edit and readback.
                                    # Failure stops this worker; it never renews authority.
                                    async with asyncio.timeout(15):
                                        message = await channel.fetch_message(int(request.message_id))
                                        if (message.author.id != self.native.user.id or message.attachments
                                                or len(message.embeds) != 1
                                                or message.embeds[0].description != request.transcript):
                                            raise RuntimeError('Ring UI source readback failed')
                                        payload = card_payload(capture_id=request.capture_id,
                                            transcript=request.transcript, expires_at=request.expires_at,
                                            audio_expires_at=request.audio_expires_at, state=request.state,
                                            route_state=route.state if route else None,
                                            route_failure_code=route.failure_code if route else None,
                                            retry_not_before=route.retry_not_before if route else None,
                                            creation_stopped=creation_stopped if route else False)
                                        if not self.thread_port.presentation_ready(request.channel_id, card_id=request.message_id):
                                            continue
                                        await message.edit(**payload, view=self.view(request))
                                        if not self.thread_port.presentation_ready(request.channel_id, card_id=request.message_id):
                                            continue
                                        confirmed = await channel.fetch_message(int(request.message_id))
                                        if (confirmed.author.id != self.native.user.id or confirmed.attachments
                                                or [embed.to_dict() for embed in confirmed.embeds] !=
                                                   [embed.to_dict() for embed in payload['embeds']]):
                                            raise RuntimeError('Ring UI edit readback failed')
                                    self.rendered[request.capture_id] = signature
                            after = requests[-1].capture_id
                except Exception:
                    self.projection_failed = True  # Terminal: notices only, never recovery.
            await asyncio.sleep(0.5)

    async def publish(self):
        while True:
            if self.native.is_ready():
                with self.store() as store:
                    if time.monotonic() >= self.next_cleanup:
                        purge_expired_audio(store, scope=self.scope, now_ms=int(time.time()*1000))
                        self.cleanup_at_ms = int(time.time()*1000)
                        self.next_cleanup = time.monotonic() + 60
                async with self.thread_port.presentation_slot(self.scope['channel_id']) as available:
                    with self.store() as store:
                        now = int(time.time()*1000)
                        request = (store.claim_card(**self.scope, now_ms=now, thread_mode=True)
                            if store.thread_global_retry_not_before(**self.scope, now_ms=now) is None
                            and available else None)
                    if request is not None:
                        channel = self.native.get_channel(int(self.scope['channel_id']))
                        if (channel is None or channel.type != discord.ChannelType.text
                                or str(channel.guild.id) != self.scope['guild_id']):
                            raise RuntimeError('Ring destination is unavailable or mismatched')
                        payload = card_payload(capture_id=request.capture_id, transcript=request.transcript,
                            expires_at=request.expires_at, audio_expires_at=request.audio_expires_at,
                            state=request.state)
                        try:
                            async with asyncio.timeout(15):
                                message = await channel.send(**payload, view=self.view(request),
                                                             nonce=request.nonce[:24])
                                with self.store() as store:
                                    store.remember_card(request.capture_id, str(message.id))
                                confirmed = await channel.fetch_message(message.id)
                                if (confirmed.author.id != self.native.user.id or confirmed.attachments
                                        or [embed.to_dict() for embed in confirmed.embeds] !=
                                           [embed.to_dict() for embed in payload['embeds']]):
                                    raise RuntimeError('Ring card readback failed')
                                with self.store() as store:
                                    store.bind_card(request.capture_id, str(message.id))
                        except BaseException:
                            with self.store() as store:
                                store.uncertain(request.capture_id)
                            raise
                with self.store() as store:
                    creation = store.claim_thread_creation(**self.scope, now_ms=int(time.time()*1000))
                if creation is not None:
                    self.known_thread_ids.add(creation.message_id)
                    create_started = False
                    try:
                        with self.store() as store:
                            route = store.get_route(creation.capture_id)
                        if route.thread_id is None:
                            message, parent, thread, member = await self.thread_port.readback(
                                creation.guild_id, creation.channel_id, creation.message_id)
                            validate_parent_card(message, creation, scope=self.scope, bot_id=self.native.user.id)
                            create_started = True
                            await self.create_claimed_thread(creation, name=f'Ring capture {creation.capture_id}')
                        await self.validate_claimed_thread(creation)
                    except asyncio.CancelledError:
                        with self.store() as store:
                            now_ms = int(time.time()*1000)
                            if (create_started or not store.defer_cancelled_preflight(
                                    creation.capture_id, expected_request=creation,
                                    **self.scope, now_ms=now_ms,
                                    retry_not_before=now_ms + 1000)):
                                store.fail_thread(creation.capture_id, failure_code='cancelled',
                                                  uncertain=create_started)
                        raise
                    except (ClosedTransport, DestinationRejected) as exc:
                        self.record_route_failure(creation, exc)
            await asyncio.sleep(0.25)

    async def stop(self):
        self.stopping = True
        self.pending_notices.clear()
        self.notice_ids.clear()
        tasks = [task for task in (self.task, self.dispatch_task, self.ui_task, self.health_task)
                 if task is not None] + list(self._dispatch_runs.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._dispatch_runs.clear()
        await self.guard.close()
        if self.health_task is not None:
            try:
                write_health(self.data_dir, self.scope, now_ms=int(time.time()*1000),
                    workers_ok=False, native_ready=False, cleanup_at_ms=self.cleanup_at_ms,
                    creation_stopped=(self.thread_port.creation_failure is not None))
            except (OSError, ValueError):
                pass  # Unwritable evidence becomes stale; stopping must still finish.
