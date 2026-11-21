"""Performance and non-blocking invariants for RelayStore and RingService polling."""
from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import discord
import pytest

from pebble_bridge.discord_ui import card_payload
from pebble_bridge.relay_store import RelayStore
from pebble_bridge.service import RingService
from test_relay_store import SCOPE, capture


def _seed_terminal_cards(tmp_path, count: int) -> list[int]:
    ids = []
    for i in range(count):
        item = capture(tmp_path, text=f'capture {i}', now=1000 + i)
        ids.append(item.id)
    with RelayStore(tmp_path) as store:
        for i, cid in enumerate(ids):
            req = store.claim_card(**SCOPE, now_ms=2000 + i, thread_mode=True)
            assert req is not None and req.capture_id == cid
            msg_id = str(423456789012345600 + i)
            store.bind_card(cid, msg_id)
            store.db.execute(
                "UPDATE relay_requests SET state='turn_finished', expires_at=1500, audio_expires_at=1500 WHERE capture_id=?",
                (cid,),
            )
            store.db.execute(
                "UPDATE relay_threads SET state='ready', thread_id=?, attempt_started_at=2500 WHERE capture_id=?",
                (msg_id, cid),
            )
    return ids


def test_relay_store_reopen_on_valid_schema_needs_no_write_transaction(tmp_path, monkeypatch):
    _seed_terminal_cards(tmp_path, 3)
    real_connect = sqlite3.connect

    def read_only_tx_connect(*args, **kwargs):
        db = real_connect(*args, **kwargs)
        def authorize(action, arg1, *_):
            if action in (sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_CREATE_TABLE):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        db.set_authorizer(authorize)
        return db

    monkeypatch.setattr(sqlite3, 'connect', read_only_tx_connect)
    with RelayStore(tmp_path) as store:
        assert store.get(1) is not None


def test_idle_polling_methods_do_not_open_write_transactions_and_join_routes(tmp_path):
    ids = _seed_terminal_cards(tmp_path, 5)
    with RelayStore(tmp_path) as store:
        expected = [(r, store.get_route(r.capture_id)) for r in store.cards(**SCOPE)]

        def deny_tx(action, *_):
            if action == sqlite3.SQLITE_TRANSACTION:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.db.set_authorizer(deny_tx)
        try:
            store.expire_due(**SCOPE, now_ms=10_000)
            assert store.claim_card(**SCOPE, now_ms=10_000, thread_mode=True) is None
            assert store.claim_thread_creation(**SCOPE, now_ms=10_000) is None
            joined = store.cards_with_routes(**SCOPE)
        finally:
            store.db.set_authorizer(None)

        assert joined == expected
        assert [r.capture_id for r, _ in joined] == ids


def _make_service(tmp_path):
    service = RingService.__new__(RingService)
    service.scope = SCOPE
    service.data_dir = tmp_path
    service.audio_retention_ms = 3600000
    service.rendered = {}
    service.known_thread_ids = set()
    service.projection_failed = False
    service.stopping = False
    service.start_attempted = False
    service.pending_notices = []
    service.notice_ids = set()
    service._dispatch_runs = {}
    service.next_cleanup = time.monotonic() + 3600
    service.cleanup_at_ms = None
    service.task = service.dispatch_task = service.ui_task = service.health_task = None
    service.native = NS(
        is_ready=lambda: True,
        user=NS(id=999999999999999999),
        add_view=Mock(),
        get_channel=Mock(return_value=None),
    )
    service.port = NS(thread_preflight=Mock())
    service.guard = NS(install=Mock())

    class _Slot:
        async def __aenter__(self):
            return True

        async def __aexit__(self, *_):
            return False

    service.thread_port = NS(
        creation_failure=None,
        presentation_ready=lambda *a, **k: True,
        presentation_slot=lambda *a, **k: _Slot(),
    )
    return service


@pytest.mark.asyncio
async def test_reconcile_uses_single_store_open_off_event_loop(tmp_path, monkeypatch):
    _seed_terminal_cards(tmp_path, 25)
    service = _make_service(tmp_path)
    with RelayStore(tmp_path) as store:
        for req, route in store.cards_with_routes(**SCOPE):
            service.rendered[req.capture_id] = (
                req.state,
                True,
                route.state if route else None,
                route.failure_code if route else None,
                route.thread_id if route else None,
                route.retry_not_before if route else None,
                False,
            )

    main_tid = threading.get_ident()
    open_tids = []
    real_store = service.store

    def counting_store():
        open_tids.append(threading.get_ident())
        return real_store()

    monkeypatch.setattr(service, 'store', counting_store)

    async def stop_after_one_tick(_delay):
        raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, 'sleep', stop_after_one_tick)
    with pytest.raises(asyncio.CancelledError):
        await service.reconcile()

    assert service.projection_failed is False
    assert len(open_tids) == 1
    assert all(tid != main_tid for tid in open_tids)


def test_start_uses_single_store_open_across_all_cards(tmp_path, monkeypatch):
    _seed_terminal_cards(tmp_path, 15)
    service = _make_service(tmp_path)
    opens = 0
    real_store = service.store

    def counting_store():
        nonlocal opens
        opens += 1
        return real_store()

    monkeypatch.setattr(service, 'store', counting_store)
    created = []

    def fake_create_task(coro, *, name=None):
        coro.close()
        task = Mock()
        task.done.return_value = False
        created.append(name)
        return task

    monkeypatch.setattr(asyncio, 'create_task', fake_create_task)
    service.start()

    assert opens == 1
    assert service.native.add_view.call_count == 15
    assert len(service.known_thread_ids) == 15


@pytest.mark.asyncio
async def test_publish_and_dispatch_offload_idle_store_polling(tmp_path, monkeypatch):
    _seed_terminal_cards(tmp_path, 5)
    service = _make_service(tmp_path)
    main_tid = threading.get_ident()
    publish_tids = []
    dispatch_tids = []
    real_store = service.store

    async def stop_after_one_tick(_delay):
        raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, 'sleep', stop_after_one_tick)

    def track_publish():
        publish_tids.append(threading.get_ident())
        return real_store()

    monkeypatch.setattr(service, 'store', track_publish)
    with pytest.raises(asyncio.CancelledError):
        await service.publish()

    assert len(publish_tids) <= 2
    assert all(tid != main_tid for tid in publish_tids)

    def track_dispatch():
        dispatch_tids.append(threading.get_ident())
        return real_store()

    monkeypatch.setattr(service, 'store', track_dispatch)
    with pytest.raises(asyncio.CancelledError):
        await service.dispatch()

    assert len(dispatch_tids) == 1
    assert all(tid != main_tid for tid in dispatch_tids)


def _discord_message_from_card(bot_id: int, payload: dict, view: discord.ui.View):
    from discord.components import _component_factory

    msg = NS(
        author=NS(id=bot_id),
        attachments=[],
        content='',
        embeds=[discord.Embed.from_dict(e.to_dict()) for e in payload['embeds']],
        components=[_component_factory(c) for c in view.to_components()],
        edit=AsyncMock(),
    )
    return msg


@pytest.mark.asyncio
async def test_reconcile_skips_edit_and_second_fetch_when_card_already_matches(tmp_path, monkeypatch):
    _seed_terminal_cards(tmp_path, 3)
    service = _make_service(tmp_path)
    bot_id = service.native.user.id
    messages = {}
    with RelayStore(tmp_path) as store:
        for req, route in store.cards_with_routes(**SCOPE):
            payload = card_payload(
                capture_id=req.capture_id, transcript=req.transcript,
                expires_at=req.expires_at, audio_expires_at=req.audio_expires_at,
                state=req.state, route_state=route.state if route else None,
                route_failure_code=route.failure_code if route else None,
                retry_not_before=route.retry_not_before if route else None,
                creation_stopped=False,
            )
            view = service.view(req, route=route, verified_thread=True)
            messages[int(req.message_id)] = _discord_message_from_card(bot_id, payload, view)

    fetch_counts = {mid: 0 for mid in messages}

    async def fetch_message(mid):
        fetch_counts[mid] += 1
        return messages[mid]

    channel = NS(fetch_message=fetch_message)
    service.native.get_channel = lambda cid: channel if cid == int(SCOPE['channel_id']) else None

    async def stop_after_one_tick(_delay):
        raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, 'sleep', stop_after_one_tick)
    with pytest.raises(asyncio.CancelledError):
        await service.reconcile()

    assert service.projection_failed is False
    assert len(service.rendered) == 3
    for mid, msg in messages.items():
        assert msg.edit.await_count == 0
        assert fetch_counts[mid] == 1


@pytest.mark.asyncio
async def test_reconcile_edits_when_button_state_or_embed_differs(tmp_path, monkeypatch):
    _seed_terminal_cards(tmp_path, 2)
    service = _make_service(tmp_path)
    bot_id = service.native.user.id
    messages = {}
    with RelayStore(tmp_path) as store:
        pairs = store.cards_with_routes(**SCOPE)
    for idx, (req, route) in enumerate(pairs):
        payload = card_payload(
            capture_id=req.capture_id, transcript=req.transcript,
            expires_at=req.expires_at, audio_expires_at=req.audio_expires_at,
            state=req.state if idx == 0 else 'pending',
            route_state=route.state if route else None,
            route_failure_code=route.failure_code if route else None,
            retry_not_before=route.retry_not_before if route else None,
            creation_stopped=False,
        )
        view = service.view(req, route=route, verified_thread=True)
        if idx == 0:
            # Embed matches, but Approve button is still enabled on Discord -> must edit!
            view.children[0].disabled = False
        msg = _discord_message_from_card(bot_id, payload, view)

        async def _apply_edit(m=msg, **kwargs):
            from discord.components import _component_factory
            m.embeds = [discord.Embed.from_dict(e.to_dict()) for e in kwargs['embeds']]
            m.components = [_component_factory(c) for c in kwargs['view'].to_components()]

        msg.edit = AsyncMock(side_effect=_apply_edit)
        messages[int(req.message_id)] = msg

    fetch_counts = {mid: 0 for mid in messages}

    async def fetch_message(mid):
        fetch_counts[mid] += 1
        return messages[mid]

    channel = NS(fetch_message=fetch_message)
    service.native.get_channel = lambda cid: channel if cid == int(SCOPE['channel_id']) else None

    async def stop_after_one_tick(_delay):
        raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, 'sleep', stop_after_one_tick)
    with pytest.raises(asyncio.CancelledError):
        await service.reconcile()

    assert service.projection_failed is False
    assert len(service.rendered) == 2
    for mid, msg in messages.items():
        assert msg.edit.await_count == 1
        assert fetch_counts[mid] == 2


@pytest.mark.asyncio
async def test_reconcile_still_fails_closed_on_tampered_source_readback(tmp_path, monkeypatch):
    _seed_terminal_cards(tmp_path, 1)
    service = _make_service(tmp_path)
    bot_id = service.native.user.id
    with RelayStore(tmp_path) as store:
        req, route = store.cards_with_routes(**SCOPE)[0]
    payload = card_payload(
        capture_id=req.capture_id, transcript='TAMPERED TRANSCRIPT',
        expires_at=req.expires_at, audio_expires_at=req.audio_expires_at,
        state=req.state, route_state=route.state,
    )
    view = service.view(req, route=route, verified_thread=True)
    msg = _discord_message_from_card(bot_id, payload, view)
    channel = NS(fetch_message=AsyncMock(return_value=msg))
    service.native.get_channel = lambda cid: channel if cid == int(SCOPE['channel_id']) else None

    async def stop_after_one_tick(_delay):
        raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, 'sleep', stop_after_one_tick)
    with pytest.raises(asyncio.CancelledError):
        await service.reconcile()

    assert service.projection_failed is True
    assert service.rendered == {}
    assert msg.edit.await_count == 0



