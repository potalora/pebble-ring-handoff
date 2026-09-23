"""Human admission policy with real SQLite, synthetic events, and no delivery."""
from types import SimpleNamespace as NS
import asyncio
import sqlite3
from unittest.mock import AsyncMock, Mock
import discord
import pytest
from pebble_bridge.relay_store import RelayStore
from pebble_bridge.service import RingService
from test_thread_store import prepared, ready, SCOPE, CARD

ORDINARY = '823456789012345678'
MESSAGE = '923456789012345678'


def human(chat=ORDINARY, command=None, **source_overrides):
    parent = chat == SCOPE['channel_id']
    source = dict(platform=NS(value='discord'), user_id=SCOPE['approver_id'],
        scope_id=SCOPE['guild_id'], is_bot=False, chat_id=chat,
        chat_type='group' if parent else 'thread', thread_id=None if parent else chat,
        parent_chat_id=None if parent else SCOPE['channel_id'])
    return NS(source=NS(**(source | source_overrides)), internal=False,
        allow_gateway_control=True, message_id=MESSAGE, get_command=lambda: command)


def policy(tmp_path):
    service = RingService.__new__(RingService)
    service.scope, service.data_dir = SCOPE, tmp_path
    service.audio_retention_ms = 3600000
    service._dispatch_permit = service._admitted_event = None
    service._dispatch_caller = service._admitted_owner = service._admitted_key = None
    service._admitted = {}
    return service


def test_card_lookup_is_exact_scoped_identity_even_without_route(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        store.db.execute('DELETE FROM relay_threads')
        assert store.card_request(CARD, **SCOPE) == store.get(req.capture_id)
        assert store.card_request(ORDINARY, **SCOPE) is None
        for field in SCOPE:
            assert store.card_request(CARD, **(SCOPE | {field: ORDINARY})) is None
        for bad in ['', '1', 123, '９' * 18, CARD + '\n', '0' + '1' * 17, str(2**64)]:
            with pytest.raises(ValueError):
                store.card_request(bad, **SCOPE)


@pytest.mark.parametrize('command', [None, 'new', 'reset'])
def test_ordinary_human_thread_admitted_without_capture_mutation(tmp_path, command):
    store, req = prepared(tmp_path)
    with store:
        before = store.get(req.capture_id)
        assert policy(tmp_path).allow_event(human(command=command))
        assert store.get(req.capture_id) == before
        assert not store.db.execute('SELECT * FROM relay_thread_resets').fetchall()


@pytest.mark.parametrize('source', [dict(user_id=ORDINARY), dict(scope_id=ORDINARY),
    dict(is_bot=True), dict(parent_chat_id=ORDINARY)])
def test_ordinary_thread_requires_exact_human_scope(tmp_path, source):
    assert not policy(tmp_path).allow_event(human(**source))


def test_parent_never_admitted_directly(tmp_path):
    assert not policy(tmp_path).allow_event(human(SCOPE['channel_id']))


@pytest.mark.parametrize('route', ['unbound', 'missing', 'mismatched'])
def test_capture_identity_never_falls_through_as_ordinary(tmp_path, route):
    store, req = prepared(tmp_path)
    with store:
        if route == 'missing':
            store.db.execute('DELETE FROM relay_threads')
        if route == 'mismatched':
            ready(store, req)
            store.db.execute('UPDATE relay_threads SET thread_id=?', (ORDINARY,))
        assert not policy(tmp_path).allow_event(human(CARD))


def test_finished_capture_and_expired_reset_lifecycle_preserved(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        ready(store, req)
        store.db.execute("UPDATE relay_requests SET state='turn_finished'")
        assert policy(tmp_path).allow_event(human(CARD))
        store.db.execute("UPDATE relay_requests SET state='expired'")
        assert not policy(tmp_path).allow_event(human(CARD))
        assert policy(tmp_path).allow_event(human(CARD, command='new'))
        assert store.thread_reset_message(CARD, **SCOPE) == MESSAGE


def test_failed_unbound_capture_thread_requires_explicit_new_then_accepts_human_input(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        assert req is not None
        store.fail_thread(req.capture_id, failure_code='cancelled', uncertain=False)
        assert store.get_route(req.capture_id).thread_id is None
        assert not policy(tmp_path).allow_event(human(CARD))
        assert policy(tmp_path).allow_event(human(CARD, command='new'))
        assert store.thread_reset_message(CARD, **SCOPE) == MESSAGE
        next_message = human(CARD)
        next_message.message_id = str(int(MESSAGE) + 1)
        assert policy(tmp_path).allow_event(next_message)
        assert store.get(req.capture_id).state == 'failed'
        assert store.get_route(req.capture_id).thread_id is None
        assert store.claim_thread_creation(**SCOPE, now_ms=5000) is None
        assert store.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=5000) is None


def test_failed_capture_notice_explains_new_command_instead_of_waiting(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        store.fail_thread(req.capture_id, failure_code='cancelled', uncertain=False)
    channel = Mock(spec=discord.Thread)
    channel.id, channel.parent_id = int(CARD), int(SCOPE['channel_id'])
    channel.guild = NS(id=int(SCOPE['guild_id']))
    channel.send = AsyncMock()
    service = policy(tmp_path)
    service.native = NS(get_channel=lambda ident: channel if ident == int(CARD) else None)
    asyncio.run(service._send_notice((CARD, MESSAGE)))
    text = channel.send.await_args.args[0]
    assert '/new' in text and 'resend' in text and 'Wait, then resend' not in text


def test_notice_keeps_generic_refusal_when_store_is_unavailable(tmp_path):
    channel = Mock(spec=discord.Thread)
    channel.id, channel.parent_id = int(CARD), int(SCOPE['channel_id'])
    channel.guild = NS(id=int(SCOPE['guild_id']))
    channel.send = AsyncMock()
    service = policy(tmp_path)
    service.native = NS(get_channel=lambda ident: channel if ident == int(CARD) else None)
    service.store = lambda: (_ for _ in ()).throw(sqlite3.OperationalError('unavailable'))
    asyncio.run(service._send_notice((CARD, MESSAGE)))
    assert 'state is unavailable' in channel.send.await_args.args[0]


def test_database_failure_is_not_ordinary_admission(tmp_path, monkeypatch):
    def unavailable(*args, **kwargs):
        raise sqlite3.OperationalError('synthetic unavailable')
    service = policy(tmp_path)
    monkeypatch.setattr(service, 'store', unavailable)
    with pytest.raises(sqlite3.OperationalError):
        service.allow_event(human())


@pytest.mark.parametrize('mapped', [False, True])
def test_hook_reset_only_touches_mapped_capture_thread(tmp_path, monkeypatch, mapped):
    from pebble_bridge import plugin
    store, req = prepared(tmp_path)
    with store:
        if mapped:
            ready(store, req)
    service = policy(tmp_path)
    service.guard = NS(_closed=False)
    service.known_thread_ids = set()
    service.offer_notice = lambda event: None
    service.on_ready = lambda: None
    service.start = lambda: None
    service.rendered = {}
    hooks, handlers = {}, {}
    config = SCOPE | dict(data_dir=str(tmp_path), audio_retention_ms=3600000)
    ctx = NS(get_config=config.get, register_skill=lambda **kwargs: None,
        register_hook=lambda name, fn: hooks.update({name: fn}),
        register_platform_handler=lambda name, fn: handlers.update({name: fn}))
    monkeypatch.setattr(plugin, 'RingService', lambda *args, **kwargs: service)
    monkeypatch.setattr(plugin, 'register_management', lambda *args: None)
    monkeypatch.setattr(plugin, 'register_audio_tool', lambda *args: None)
    monkeypatch.setattr(plugin, 'external_state_path', lambda path: True)
    plugin.register(ctx)
    adapter = object()
    handlers['discord'](NS(add_listener=lambda *args: None, is_ready=lambda: True), adapter)
    event = human(CARD if mapped else ORDINARY, command='new')
    platform = 'discord'
    event.source.platform = type('Platform', (), {'value': platform})()
    result = hooks['pre_gateway_dispatch'](event=event,
        gateway=NS(adapters={event.source.platform: adapter}))
    assert result is None
    with RelayStore(tmp_path) as store:
        assert store.get(req.capture_id).state == ('reset' if mapped else 'approved_waiting')
        assert len(store.db.execute('SELECT * FROM relay_thread_resets').fetchall()) == int(mapped)


@pytest.mark.asyncio
async def test_guard_database_failure_never_reaches_native_lane(tmp_path, monkeypatch):
    from pebble_bridge.admission_guard import AdmissionGuard
    delivered, notices = [], []
    async def handle_message(event):
        delivered.append(event)
    async def notify(event):
        notices.append(event.message_id)
    service = policy(tmp_path)
    service.known_thread_ids = set()
    def unavailable():
        raise sqlite3.OperationalError('synthetic unavailable')
    monkeypatch.setattr(service, 'store', unavailable)
    adapter = NS(handle_message=handle_message)
    guard = AdmissionGuard(adapter, is_ring_scope=service.is_ring_scope,
        allow=service.allow_event, notify=notify)
    guard.install()
    try:
        await adapter.handle_message(human())
        assert not delivered
        assert notices == [MESSAGE]
    finally:
        await guard.close()


def test_authenticated_parent_input_reaches_thread_router_without_capture_authority(tmp_path, monkeypatch):
    """The pre-dispatch hook must leave a human parent message for the async guard."""
    from pebble_bridge import plugin
    store, req = prepared(tmp_path)
    with store:
        before = (store.get(req.capture_id), store.get_route(req.capture_id))
    service = policy(tmp_path)
    service.guard = NS(_closed=False)
    service.known_thread_ids = set()
    notices = []
    service.offer_notice = notices.append
    service.on_ready = lambda: None
    service.start = lambda: None
    service.rendered = {}
    hooks, handlers = {}, {}
    config = SCOPE | dict(data_dir=str(tmp_path), audio_retention_ms=3600000)
    ctx = NS(get_config=config.get, register_skill=lambda **kwargs: None,
        register_hook=lambda name, fn: hooks.update({name: fn}),
        register_platform_handler=lambda name, fn: handlers.update({name: fn}))
    monkeypatch.setattr(plugin, 'RingService', lambda *args, **kwargs: service)
    monkeypatch.setattr(plugin, 'register_management', lambda *args: None)
    monkeypatch.setattr(plugin, 'register_audio_tool', lambda *args: None)
    monkeypatch.setattr(plugin, 'external_state_path', lambda path: True)
    plugin.register(ctx)
    adapter = object()
    handlers['discord'](NS(add_listener=lambda *args: None, is_ready=lambda: True), adapter)
    event = human(SCOPE['channel_id'])
    event.source.platform = type('Platform', (), {'value': 'discord'})()
    event.text = 'A new typed question'
    result = hooks['pre_gateway_dispatch'](event=event,
        gateway=NS(adapters={event.source.platform: adapter}))
    assert result is None
    assert notices == []
    with RelayStore(tmp_path) as store:
        assert (store.get(req.capture_id), store.get_route(req.capture_id)) == before
        assert store.db.execute('SELECT count(*) FROM relay_audio_grants').fetchone()[0] == 0


def test_unconfigured_install_registers_only_inert_catalog_surfaces():
    import json
    from pebble_bridge import plugin
    tools, hooks, handlers = {}, {}, {}
    ctx = NS(get_config=lambda key: None, register_skill=lambda **kwargs: None,
        register_cli_command=lambda **kwargs: None,
        register_tool=lambda **kwargs: tools.update({kwargs['name']: kwargs['handler']}),
        register_hook=lambda name, fn: hooks.update({name: fn}),
        register_platform_handler=lambda name, fn: handlers.update({name: fn}))
    plugin.register(ctx)
    assert set(tools) == {'pebble_status', 'ring_get_audio'}
    assert set(hooks) == {'pre_gateway_dispatch'}
    assert not handlers
    assert json.loads(tools['pebble_status']({}))['status'] == 'configuration-unavailable'
    assert 'error' in json.loads(tools['ring_get_audio']({'capture_id': 1}))


def test_partial_install_configuration_does_not_wire_ring():
    from pebble_bridge import plugin
    ctx = NS(get_config=lambda key: '/tmp/ring-data' if key == 'data_dir' else None)
    with pytest.raises(ValueError, match='approver'):
        plugin.register(ctx)


@pytest.mark.asyncio
async def test_verified_parent_audio_request_grants_only_routed_thread_identity(tmp_path, monkeypatch):
    from pebble_bridge import human_threads, service as service_module
    store, req = prepared(tmp_path)
    store.db.close()
    parent = human(SCOPE['channel_id'])
    parent.text = f'check the recording for capture {req.capture_id}'
    parent.raw_message = NS(id=int(MESSAGE), content=parent.text,
        author=NS(id=int(SCOPE['approver_id']), bot=False))
    parent.source.profile = 'default'
    routed = human(ORDINARY)
    routed.text, routed.message_id, routed.raw_message = (
        parent.text, parent.message_id, parent.raw_message)
    routed.source.profile = 'default'

    async def route(_service, event):
        assert event is parent
        return routed

    monkeypatch.setattr(human_threads, 'route_human_event', route)
    monkeypatch.setattr(service_module.time, 'time', lambda: 2.0)
    ring = policy(tmp_path)
    assert await RingService.route_human_event(ring, parent) is routed
    with RelayStore(tmp_path) as store:
        granted = store.db.execute('SELECT chat_id, message_id FROM relay_audio_grants').fetchall()
        assert [(row['chat_id'], row['message_id']) for row in granted] == [(ORDINARY, MESSAGE)]
