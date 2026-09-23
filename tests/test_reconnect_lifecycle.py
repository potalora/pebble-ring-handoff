"""A fresh Discord adapter must own the only live Ring service and health writer."""
import asyncio
from types import SimpleNamespace as NS

import pytest

from pebble_bridge import plugin
SCOPE = {'guild_id': '323456789012345678',
         'channel_id': '223456789012345678',
         'approver_id': '123456789012345678'}


@pytest.mark.parametrize('fail_stop', [False, True])
@pytest.mark.asyncio
async def test_reconnect_stops_old_service_before_new_start_and_keeps_confirmed_projection(
        tmp_path, monkeypatch, fail_stop):
    events = []
    release_stop = asyncio.Event()
    instances = []

    class Service:
        def __init__(self, native, adapter, **_kwargs):
            self.index = len(instances) + 1
            self.native = native
            self.adapter = adapter
            self.rendered = {}
            self.known_thread_ids = set()
            self.active = False
            instances.append(self)

        def start(self):
            self.active = True
            events.append(f'start-{self.index}')

        async def on_ready(self):
            self.ready_callback()

        def is_ring_scope(self, event):
            return event.source.chat_id in self.known_thread_ids

        async def stop(self):
            events.append(f'stop-begin-{self.index}')
            await release_stop.wait()
            self.active = False
            events.append(f'stop-end-{self.index}')
            if fail_stop:
                raise RuntimeError('synthetic stop failure')

    monkeypatch.setattr(plugin, 'RingService', Service)
    monkeypatch.setattr(plugin, 'register_management', lambda *args: None)
    monkeypatch.setattr(plugin, 'register_audio_tool', lambda *args: None)
    monkeypatch.setattr(plugin, 'external_state_path', lambda path: True)
    hooks, handlers = {}, {}
    config = SCOPE | dict(data_dir=str(tmp_path), audio_retention_ms=3600000)
    ctx = NS(get_config=config.get, register_skill=lambda **kwargs: None,
        register_hook=lambda name, fn: hooks.update({name: fn}),
        register_platform_handler=lambda name, fn: handlers.update({name: fn}))
    plugin.register(ctx)

    class Native:
        def __init__(self):
            self.listeners = []

        def add_listener(self, callback, event):
            assert event == 'on_ready'
            self.listeners.append(callback)

        def is_ready(self):
            return True

    first_native, second_native = Native(), Native()
    first_adapter, second_adapter = object(), object()
    handlers['discord'](first_native, first_adapter)
    assert events == ['start-1']
    await first_native.listeners[0]()
    assert events == ['start-1', 'start-1']  # Native ready rechecks worker health.
    instances[0].rendered[42] = ('confirmed-card-signature',)
    card_thread = '523456789012345678'
    instances[0].known_thread_ids.add(card_thread)

    handlers['discord'](second_native, second_adapter)
    await asyncio.sleep(0)
    assert 'stop-begin-1' in events
    assert 'start-2' not in events
    platform = type('Platform', (), {'value': 'discord'})()
    stale_thread = NS(source=NS(platform=platform, scope_id=SCOPE['guild_id'],
        chat_id=card_thread, parent_chat_id=None))
    assert hooks['pre_gateway_dispatch'](event=stale_thread,
        gateway=NS(adapters={platform: second_adapter})) == {
            'action': 'skip', 'reason': 'Ring admission service unavailable'}
    await second_native.listeners[0]()
    assert 'start-2' not in events
    if fail_stop:
        # A third adapter can wire while the first retirement is still pending.
        handlers['discord'](Native(), object())

    release_stop.set()
    async with asyncio.timeout(2):
        while 'stop-end-1' not in events:
            await asyncio.sleep(0.01)
    await asyncio.sleep(0)
    if fail_stop:
        await asyncio.sleep(0.01)
        assert 'start-2' not in events and 'start-3' not in events
        with pytest.raises(RuntimeError, match='retirement failed'):
            handlers['discord'](Native(), object())
        return
    async with asyncio.timeout(2):
        while 'start-2' not in events:
            await asyncio.sleep(0.01)
    assert events.index('stop-end-1') < events.index('start-2')
    assert instances[0].active is False
    assert instances[1].active is True
    assert instances[1].rendered == {42: ('confirmed-card-signature',)}
