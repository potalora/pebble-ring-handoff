"""Successor plugin factory. All configuration is plugin-local and explicit."""
import asyncio
import logging
from pathlib import Path
import re
import sqlite3
import time

from .service import RingService
from .relay_store import RelayStore
from .audio_tools import grant_from_event, register_audio_tool
from .management import external_state_path, register_management

logger = logging.getLogger(__name__)


def register(ctx):
    location = ctx.get_config('data_dir')
    scope = {key: ctx.get_config(key) for key in ('guild_id','channel_id','approver_id')}
    retention = ctx.get_config('audio_retention_ms')
    if location is None and retention is None and all(value is None for value in scope.values()):
        # A fresh catalog install has no operator settings. Expose only inert
        # discovery surfaces; the native handler is never wired until configured.
        register_management(ctx, None, scope)
        ctx.register_skill(name='pebble',
            path=Path(__file__).resolve().parents[1]/'skills/pebble/SKILL.md',
            description='Check Ring status and the operator recovery procedure.')
        register_audio_tool(ctx, None)
        ctx.register_hook('pre_gateway_dispatch', lambda **_kwargs: None)
        return
    if not isinstance(location, str) or not Path(location).is_absolute():
        raise ValueError('Ring requires an explicit absolute data directory')
    data_dir = Path(location)
    runtime_root = data_dir.parent if data_dir.name == 'data' else data_dir
    if not external_state_path(runtime_root) or not external_state_path(data_dir):
        raise ValueError('Ring data must remain outside the replaceable plugin tree')
    if any(not isinstance(value, str) or re.fullmatch(r'[0-9]{17,20}', value) is None
           for value in scope.values()):
        raise ValueError('Ring requires one exact numeric approver and destination')
    if type(retention) is not int or not 0 < retention <= 30*24*60*60*1000:
        raise ValueError('Ring requires explicit bounded audio retention')

    services = {}
    replacement_lock = asyncio.Lock()
    replacement_pending = False
    retirement_failed = False
    generation = 0
    confirmed_rendered = {}
    known_ring_threads = set()

    def wire(native, adapter):
        nonlocal generation, replacement_pending
        if retirement_failed:
            raise RuntimeError('Ring prior service retirement failed; restart required')
        generation += 1
        current_generation = generation
        service = RingService(native, adapter, data_dir=data_dir, scope=scope,
                              audio_retention_ms=retention)
        may_start = False

        def activate():
            if current_generation != generation:
                return
            if services.get(adapter) is service:
                service.start()  # Preserve native ready's stopped-worker check.
                return
            service.rendered.update(confirmed_rendered)
            service.start()
            services[adapter] = service

        def on_ready_activation():
            if may_start and current_generation == generation:
                activate()

        service.ready_callback = on_ready_activation
        native.add_listener(service.on_ready, 'on_ready')
        if not services and not replacement_pending:
            # Initial wiring remains synchronous so startup failures reach the
            # native factory error handler. Until start succeeds, the hook skips.
            may_start = True
            if native.is_ready():
                activate()
            return

        replacement_pending = True

        async def replace_service():
            nonlocal may_start, replacement_pending, retirement_failed
            async with replacement_lock:
                if current_generation != generation:
                    return
                if retirement_failed:
                    replacement_pending = False
                    return
                prior_services = list(services.values())
                for prior in prior_services:
                    known_ring_threads.update(prior.known_thread_ids)
                services.clear()  # No native admission while the old guard closes.
                try:
                    for prior in prior_services:
                        async with asyncio.timeout(15):
                            await prior.stop()
                        confirmed_rendered.update(prior.rendered)
                    if current_generation != generation:
                        return
                    may_start = True
                    if native.is_ready():
                        activate()
                except Exception as exc:
                    # A failed handoff leaves Ring unavailable across later reconnects.
                    retirement_failed = True
                    logger.error('Ring service replacement failed (%s)', type(exc).__name__)
                finally:
                    if current_generation == generation:
                        replacement_pending = False

        asyncio.create_task(replace_service(), name='ring-service-replacement')

    def pre_dispatch(*, event, gateway=None, **_kwargs):
        source = event.source
        adapter = getattr(gateway, 'adapters', {}).get(source.platform)
        service = services.get(adapter)
        identified = (getattr(source.platform, 'value', None) == 'discord'
            and ((source.scope_id == scope['guild_id'] and
                  (source.chat_id == scope['channel_id'] or
                   source.parent_chat_id == scope['channel_id']))
                 or source.chat_id in known_ring_threads
                 or getattr(source, 'thread_id', None) in known_ring_threads
                 or any(item.is_ring_scope(event) for item in services.values())))
        if not identified:
            return None
        if service is None or service.guard._closed:
            return {'action': 'skip', 'reason': 'Ring admission service unavailable'}
        try:
            # Parent input is routed and authenticated by the async admission guard.
            # It cannot satisfy thread policy until that route has been verified.
            parent_human = (source.chat_id == scope['channel_id']
                            and service.authenticated(event))
            if not parent_human and not service.allow_event(event):
                service.offer_notice(event)
                return {'action': 'skip', 'reason': 'Ring input not admitted'}
            if not service.authenticated(event):
                return None  # Exact native-owned capture: never an audio grant.
            if event.get_command() in {'new', 'reset'}:
                with RelayStore(data_dir) as store:
                    if (source.chat_type == 'thread'
                            and store.thread_request(source.chat_id, **scope) is not None):
                        store.reset_thread(source.chat_id, **scope,
                                           reset_message_id=event.message_id)
                    elif source.chat_type != 'thread':
                        store.reset(**scope)
            elif not parent_human:
                grant_from_event(data_dir, event, scope=scope, now_ms=int(time.time()*1000))
        except (sqlite3.Error, OSError, RuntimeError, ValueError):
            # The installed hook is synchronous; no detached notice task.
            service.offer_notice(event)
            return {'action': 'skip', 'reason': 'Ring state unavailable'}
        return None

    register_management(ctx, data_dir, scope)
    ctx.register_skill(
        name='pebble',
        path=Path(__file__).resolve().parents[1]/'skills/pebble/SKILL.md',
        description='Operate Pebble through Hermes: status and approved ingress recovery.')
    register_audio_tool(ctx, data_dir)
    ctx.register_hook('pre_gateway_dispatch', pre_dispatch)
    ctx.register_platform_handler('discord', wire)
