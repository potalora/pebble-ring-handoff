import sqlite3
from dataclasses import FrozenInstanceError

import pytest

from pebble_bridge.relay_store import RelayStore
from test_relay_store import SCOPE, capture, decision

CARD = '523456789012345678'


def ready(store, req):
    assert store.claim_thread_creation(**SCOPE, now_ms=4000).capture_id == req.capture_id
    store.remember_thread(req.capture_id, req.message_id)
    assert store.ready_thread(req.capture_id, **SCOPE, now_ms=4001)


@pytest.mark.parametrize('stop', [None, 'reset', 'expired', 'captures.transcript', 'relay_requests.transcript', 'relay_requests.digest'])
def test_exact_dispatch_revalidates_without_reselection(tmp_path, stop):
    store, requests = creation_batch(tmp_path, ['valid', 'valid'])
    req, sibling = requests
    with store, RelayStore(tmp_path) as other:
        for item in requests:
            ready(store, item)
        assert len(store.thread_candidates(**SCOPE, now_ms=5000)) == 2
        for field in SCOPE:
            assert store.claim_thread_dispatch(req.capture_id, **(SCOPE | {field: '923456789012345678'}), now_ms=5000) is None
        if stop == 'reset':
            store.reset(**SCOPE)
        elif stop and '.' in stop:
            table, col = stop.split('.')
            key = 'id' if table == 'captures' else 'capture_id'
            store.db.execute(f"UPDATE {table} SET {col}='tampered' WHERE {key}=?", (req.capture_id,))
            before = store.get(req.capture_id)
            assert store.thread_candidates(**SCOPE, now_ms=5000)
            assert store.get(req.capture_id) == before
        claimed = store.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=req.expires_at if stop == 'expired' else 5001)
        assert (claimed is not None) is (stop is None)
        expected = 'dispatching' if stop is None else 'reset' if stop == 'reset' else 'expired' if stop == 'expired' else 'uncertain'
        assert other.get(req.capture_id).state == expected
        assert other.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=5002) is None
        assert other.get(sibling.capture_id).state == ('reset' if stop == 'reset' else 'approved_waiting')
        assert not store.db.in_transaction
        assert store.claim_thread_dispatch(999999, **SCOPE, now_ms=5002) is None


@pytest.mark.parametrize('state', ['unbound', 'creating', 'uncertain', 'ready', 'failed'])
@pytest.mark.parametrize('bound', [False, True])
def test_scoped_thread_lookup_is_identity_not_authority(tmp_path, state, bound):
    store, req = prepared(tmp_path)
    with store:
        if state != 'unbound':
            store.claim_thread_creation(**SCOPE, now_ms=4000)
        if bound:
            store.db.execute('UPDATE relay_threads SET thread_id=?', (CARD,))
        store.db.execute('UPDATE relay_threads SET state=?', (state,))
        store.db.execute("UPDATE relay_requests SET state='turn_finished'")
        expected = state != 'unbound' and (bound or state in ('creating', 'uncertain'))
        assert (store.thread_request(CARD, **SCOPE) is not None) is expected
        for field in SCOPE:
            assert store.thread_request(CARD, **(SCOPE | {field: '923456789012345678'})) is None
        assert store.thread_request('623456789012345678', **SCOPE) is None
        for bad in ['', '123', 523456789012345678, '９' * 18, CARD + '\n']:
            with pytest.raises(ValueError):
                store.thread_request(bad, **SCOPE)
        store.db.execute("UPDATE relay_requests SET message_id='623456789012345678'")
        assert store.thread_request(CARD, **SCOPE) is None
        store.db.execute('UPDATE relay_requests SET message_id=?', (CARD,))
        store.db.execute("UPDATE relay_threads SET thread_id='623456789012345678'")
        assert store.thread_request(CARD, **SCOPE) is None
        store.db.execute('UPDATE relay_threads SET thread_id=?,attempt_started_at=NULL', (CARD,))
        assert store.thread_request(CARD, **SCOPE) is None


def test_cancelled_preflight_waits_then_reclaims_without_retrying_failed_capture(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        assert req is not None
        assert store.defer_cancelled_preflight(req.capture_id, expected_request=req,
            **SCOPE, now_ms=4001, retry_not_before=5001)
        assert store.get(req.capture_id).state == 'approved_waiting'
        route = store.get_route(req.capture_id)
        assert (route.state, route.thread_id, route.failure_code, route.retry_not_before) == (
            'rate_wait', None, 'preflight_cancelled', 5001)
        assert store.claim_thread_creation(**SCOPE, now_ms=5000) is None
        assert store.claim_thread_creation(**SCOPE, now_ms=5001) == req
        store.fail_thread(req.capture_id, failure_code='cancelled', uncertain=False)
        assert store.claim_thread_creation(**SCOPE, now_ms=6000) is None


def test_cancelled_preflight_never_revives_reset_or_mismatched_capture(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        assert req is not None
        for field in SCOPE:
            assert not store.defer_cancelled_preflight(req.capture_id, expected_request=req,
                **(SCOPE | {field: '923456789012345678'}), now_ms=4001,
                retry_not_before=5001)
        store.reset_thread(CARD, **SCOPE)
        assert not store.defer_cancelled_preflight(req.capture_id, expected_request=req,
            **SCOPE, now_ms=4001, retry_not_before=5001)
        assert store.get_route(req.capture_id).state == 'creating'
        assert store.claim_thread_creation(**SCOPE, now_ms=5001) is None


def test_cancelled_preflight_does_not_extend_expired_approval(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        assert not store.defer_cancelled_preflight(req.capture_id, expected_request=req,
            **SCOPE, now_ms=req.expires_at, retry_not_before=req.expires_at + 1000)
        assert store.get(req.capture_id).state == 'expired'
        assert store.get_route(req.capture_id).state == 'creating'
        assert store.claim_thread_creation(**SCOPE, now_ms=req.expires_at + 1000) is None


def test_failed_unbound_thread_lookup_rejects_inconsistent_finished_request(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        store.fail_thread(req.capture_id, failure_code='cancelled', uncertain=False)
        assert store.thread_request(CARD, **SCOPE) == store.get(req.capture_id)
        store.db.execute("UPDATE relay_requests SET state='turn_finished' WHERE capture_id=?",
                         (req.capture_id,))
        assert store.thread_request(CARD, **SCOPE) is None


@pytest.mark.parametrize('state', ['approved_waiting', 'dispatching', 'turn_finished', 'expired', 'uncertain', 'failed'])
def test_thread_reset_local_authority_and_actual_chat_grants(tmp_path, state):
    store, requests = creation_batch(tmp_path, ['valid', 'valid'])
    req, sibling = requests
    with store:
        for item in requests:
            ready(store, item)
        store.db.execute('UPDATE relay_requests SET state=? WHERE capture_id=?', (state, req.capture_id))
        before_routes = [store.get_route(r.capture_id) for r in requests]
        # Synthetic grants include another capture in this chat: actual chat wins.
        grants = [(req.capture_id, CARD, SCOPE), (sibling.capture_id, CARD, SCOPE),
                  (req.capture_id, SCOPE['channel_id'], SCOPE), (sibling.capture_id, sibling.message_id, SCOPE)]
        grants += [(req.capture_id, CARD, SCOPE | {field: '923456789012345678'}) for field in SCOPE]
        for index, (cid, chat, scope) in enumerate(grants):
            store.db.execute('INSERT INTO relay_audio_grants VALUES(?,?,?,?,?,?,?,?,?,0)',
                (cid, scope['approver_id'], scope['guild_id'], chat, f'grant-{index}', 'synthetic', 'play', scope['channel_id'], 999999))
        for field in SCOPE:
            store.reset_thread(CARD, **(SCOPE | {field: '923456789012345678'}))
        assert all(row[0] == 0 for row in store.db.execute('SELECT used FROM relay_audio_grants'))
        before_unknown = {
            table: [tuple(row) for row in store.db.execute(f'SELECT * FROM {table} ORDER BY rowid')]
            for table in ('relay_requests', 'relay_threads', 'relay_audio_grants')
        }
        store.reset_thread('623456789012345678', **SCOPE)
        assert {
            table: [tuple(row) for row in store.db.execute(f'SELECT * FROM {table} ORDER BY rowid')]
            for table in before_unknown
        } == before_unknown
        store.reset_thread(CARD, **SCOPE)
        assert store.get(req.capture_id).state == ('reset' if state == 'approved_waiting' else state)
        assert store.get(sibling.capture_id).state == 'approved_waiting'
        assert [store.get_route(r.capture_id) for r in requests] == before_routes
        assert [row[0] for row in store.db.execute('SELECT used FROM relay_audio_grants ORDER BY rowid')] == [1, 1, 0, 0, 0, 0, 0]
        assert store.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=5001) is None
        store.reset_thread(CARD, **SCOPE)
        assert store.thread_request(CARD, **SCOPE).state == ('reset' if state == 'approved_waiting' else state)


@pytest.mark.parametrize('route_state', ['creating', 'uncertain'])
def test_pre_id_thread_reset_keeps_late_identity_inert(tmp_path, route_state):
    store, requests = creation_batch(tmp_path, ['valid', 'valid'])
    req, sibling = requests
    with store:
        assert store.claim_thread_creation(**SCOPE, now_ms=4000).capture_id == req.capture_id
        ready(store, sibling)
        if route_state == 'uncertain':
            # Synthetic pre-ID uncertain route; approval remains unstarted.
            store.db.execute("UPDATE relay_threads SET state='uncertain',failure_code='interrupted' WHERE capture_id=?", (req.capture_id,))
        before_route = store.get_route(req.capture_id)
        assert (before_route.state, before_route.thread_id, before_route.attempt_started_at) == (route_state, None, 4000)
        before_sibling = (store.get(sibling.capture_id), store.get_route(sibling.capture_id))
        grants = [(req.capture_id, CARD, SCOPE), (sibling.capture_id, CARD, SCOPE),
                  (req.capture_id, SCOPE['channel_id'], SCOPE), (sibling.capture_id, sibling.message_id, SCOPE)]
        grants += [(req.capture_id, CARD, SCOPE | {field: '923456789012345678'}) for field in SCOPE]
        for index, (cid, chat, scope) in enumerate(grants):
            store.db.execute('INSERT INTO relay_audio_grants VALUES(?,?,?,?,?,?,?,?,?,0)',
                (cid, scope['approver_id'], scope['guild_id'], chat, f'pre-id-{index}', 'synthetic', 'play', scope['channel_id'], 999999))
        before_grants = [tuple(row) for row in store.db.execute('SELECT * FROM relay_audio_grants ORDER BY rowid')]
        assert store.thread_request(req.message_id, **SCOPE).capture_id == req.capture_id
        store.reset_thread(req.message_id, **SCOPE)
        reset_request = store.get(req.capture_id)
        assert reset_request.state == 'reset'
        assert reset_request.expires_at == req.expires_at
        assert store.get_route(req.capture_id) == before_route
        expected_grants = [row[:-1] + (1,) if index < 2 else row for index, row in enumerate(before_grants)]
        assert [tuple(row) for row in store.db.execute('SELECT * FROM relay_audio_grants ORDER BY rowid')] == expected_grants
        store.remember_thread(req.capture_id, req.message_id)
        late_route = store.get_route(req.capture_id)
        assert late_route.thread_id == req.message_id
        assert (late_route.state, late_route.attempt_started_at, late_route.failure_code) == (
            before_route.state, before_route.attempt_started_at, before_route.failure_code)
        assert store.ready_thread(req.capture_id, **SCOPE, now_ms=5000) is False
        assert store.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=5001) is None
        assert store.get(req.capture_id) == reset_request
        assert store.get_route(req.capture_id) == late_route
        assert store.thread_request(req.message_id, **SCOPE) == reset_request
        assert (store.get(sibling.capture_id), store.get_route(sibling.capture_id)) == before_sibling
        assert [r.capture_id for r in store.thread_candidates(**SCOPE, now_ms=5001)] == [sibling.capture_id]
        assert [tuple(row) for row in store.db.execute('SELECT * FROM relay_audio_grants ORDER BY rowid')] == expected_grants


@pytest.mark.parametrize('bound', [False, True])
@pytest.mark.parametrize('state', ['approved_waiting', 'expired', 'reset', 'failed', 'turn_finished', 'posting', 'dispatching'])
@pytest.mark.parametrize('due', [False, True])
def test_recover_interrupted_creation_preserves_evidence_and_terminal_outcomes(tmp_path, bound, state, due):
    store, req = prepared(tmp_path)
    with store:
        store.claim_thread_creation(**SCOPE, now_ms=4000)
        if bound:
            store.remember_thread(req.capture_id, CARD)
        store.db.execute('UPDATE relay_requests SET state=?', (state,))
        before = (store.get(req.capture_id), store.get_route(req.capture_id))
        for field in SCOPE:
            store.recover(**(SCOPE | {field: '923456789012345678'}), now_ms=req.expires_at)
            assert (store.get(req.capture_id), store.get_route(req.capture_id)) == before
        store.recover(**SCOPE, now_ms=req.expires_at if due else 5000)
        route = store.get_route(req.capture_id)
        assert (route.state, route.failure_code, route.thread_id, route.attempt_started_at) == ('uncertain', 'interrupted', CARD if bound else None, 4000)
        expected = 'uncertain' if state in ('posting', 'dispatching') else ('expired' if due else 'uncertain') if state == 'approved_waiting' else state
        assert store.get(req.capture_id).state == expected
        store.recover(**SCOPE, now_ms=req.expires_at + 1)
        assert store.get(req.capture_id).state == expected
        assert store.get_route(req.capture_id) == route
        assert store.claim_thread_creation(**SCOPE, now_ms=5001) is None
        assert store.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=5001) is None
        assert store.thread_request(CARD, **SCOPE).capture_id == req.capture_id


@pytest.mark.parametrize('due', [False, True])
def test_recover_unattempted_keeps_original_deadline(tmp_path, due):
    store, req = prepared(tmp_path)
    with store:
        route = store.get_route(req.capture_id)
        store.recover(**SCOPE, now_ms=req.expires_at if due else 5000)
        assert store.get_route(req.capture_id) == route
        assert store.get(req.capture_id).expires_at == req.expires_at
        assert store.get(req.capture_id).state == ('expired' if due else 'approved_waiting')
        assert (store.claim_thread_creation(**SCOPE, now_ms=5001) is not None) is (not due)


def test_candidates_bounded_readonly_pagination(tmp_path):
    store, requests = creation_batch(tmp_path, ['valid'] * 102)
    with store:
        for req in requests:
            ready(store, req)
        statements = []
        store.db.set_trace_callback(statements.append)
        page = store.thread_candidates(**SCOPE, now_ms=5000)
        assert [r.capture_id for r in page] == [r.capture_id for r in requests[:100]]
        assert [r.capture_id for r in store.thread_candidates(**SCOPE, now_ms=5000, after=page[-1].capture_id)] == [r.capture_id for r in requests[100:]]
        assert not mutations(statements)
        assert not store.db.in_transaction
        for field in SCOPE:
            assert store.thread_candidates(**(SCOPE | {field: '923456789012345678'}), now_ms=5000) == []
        assert store.thread_candidates(**SCOPE, now_ms=requests[0].expires_at) == []


@pytest.mark.parametrize('invalid', ['missing', 'unbound', 'creating', 'uncertain', 'failed', 'attemptless', 'no_id', 'wrong_id', 'malformed'])
def test_candidates_exclude_invalid_routes(tmp_path, invalid):
    store, req = prepared(tmp_path)
    with store:
        ready(store, req)
        if invalid == 'missing':
            store.db.execute('DELETE FROM relay_threads')
        elif invalid in ('unbound', 'creating', 'uncertain', 'failed'):
            store.db.execute('UPDATE relay_threads SET state=?', (invalid,))
        elif invalid == 'attemptless':
            store.db.execute('UPDATE relay_threads SET attempt_started_at=NULL')
        elif invalid == 'no_id':
            store.db.execute('UPDATE relay_threads SET thread_id=NULL')
        elif invalid == 'wrong_id':
            store.db.execute("UPDATE relay_threads SET thread_id='623456789012345678'")
        else:
            store.db.execute("UPDATE relay_threads SET thread_id='bad'")
            store.db.execute("UPDATE relay_requests SET message_id='bad'")
        assert store.thread_candidates(**SCOPE, now_ms=5000) == []
        assert store.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=5000) is None
        assert store.get(req.capture_id).state == 'approved_waiting'


@pytest.mark.parametrize('operation', ['dispatch', 'reset', 'recover'])
@pytest.mark.parametrize('error', ['commit', 'lock'])
def test_p2b_writes_rollback_and_propagate(tmp_path, operation, error):
    store, req = prepared(tmp_path)
    with store, RelayStore(tmp_path) as other:
        if operation == 'recover':
            store.claim_thread_creation(**SCOPE, now_ms=4000)
        else:
            ready(store, req)
        store.db.execute('INSERT INTO relay_audio_grants VALUES(?,?,?,?,?,?,?,?,?,0)',
            (req.capture_id, SCOPE['approver_id'], SCOPE['guild_id'], CARD, 'synthetic-grant', 'synthetic', 'play', SCOPE['channel_id'], 999999))
        before = (store.get(req.capture_id), store.get_route(req.capture_id), list(store.db.execute('SELECT * FROM relay_audio_grants')))
        if error == 'commit':
            store.db.set_authorizer(lambda action, arg1, *_: sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and arg1 == 'COMMIT' else sqlite3.SQLITE_OK)
        else:
            other.db.execute('BEGIN IMMEDIATE')
        try:
            with pytest.raises(sqlite3.DatabaseError):
                if operation == 'dispatch':
                    store.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=5000)
                elif operation == 'reset':
                    store.reset_thread(CARD, **SCOPE)
                else:
                    store.recover(**SCOPE, now_ms=5000)
        finally:
            store.db.set_authorizer(None)
            if other.db.in_transaction:
                other.db.execute('ROLLBACK')
        assert not store.db.in_transaction
        assert (store.get(req.capture_id), store.get_route(req.capture_id), list(store.db.execute('SELECT * FROM relay_audio_grants'))) == before


@pytest.mark.parametrize('replacement', [
    'capture_id TEXT PRIMARY KEY REFERENCES relay_requests(capture_id)',
    'capture_id INTEGER PRIMARY KEY REFERENCES captures(id)',
    'thread_id TEXT', 'thread_id INTEGER UNIQUE',
    "state TEXT NOT NULL", "state TEXT NOT NULL CHECK(state IN ('unbound','creating','ready','failed','uncertain','other'))",
    "state TEXT NOT NULL CHECK(state IN ('un bound','creating','ready','failed','uncertain'))",
    'attempt_started_at TEXT', 'failure_code TEXT, extra TEXT',
])
def test_incompatible_schema_rejected(tmp_path, replacement):
    capture(tmp_path)
    sql = """CREATE TABLE relay_threads (
        capture_id INTEGER PRIMARY KEY REFERENCES relay_requests(capture_id),
        thread_id TEXT UNIQUE,
        state TEXT NOT NULL CHECK(state IN ('unbound','creating','ready','failed','uncertain')),
        attempt_started_at INTEGER, failure_code TEXT)"""
    original = next(x for x in [
        'capture_id INTEGER PRIMARY KEY REFERENCES relay_requests(capture_id)',
        'thread_id TEXT UNIQUE',
        "state TEXT NOT NULL CHECK(state IN ('unbound','creating','ready','failed','uncertain'))",
        'attempt_started_at INTEGER', 'failure_code TEXT'
    ] if x.split()[0] == replacement.split()[0])
    with sqlite3.connect(tmp_path / 'bridge.sqlite3') as db:
        db.execute(sql.replace(original, replacement))
    with pytest.raises(RuntimeError, match='schema'):
        RelayStore(tmp_path)


def prepared(tmp_path, approve=True):
    capture(tmp_path)
    store = RelayStore(tmp_path)
    req = store.claim_card(**SCOPE, now_ms=2000, thread_mode=True)
    req = store.bind_card(req.capture_id, CARD)
    if approve:
        store.decide(**decision(req))
    return store, req


def test_creation_is_approved_scoped_one_attempt_across_connections(tmp_path):
    store, req = prepared(tmp_path, approve=False)
    with store, RelayStore(tmp_path) as other:
        assert store.claim_thread_creation(**SCOPE, now_ms=4000) is None
        store.decide(**decision(req))
        for field in SCOPE:
            assert store.claim_thread_creation(**(SCOPE | {field: '923456789012345678'}), now_ms=4000) is None
        assert store.claim_thread_creation(**SCOPE, now_ms=4000).capture_id == req.capture_id
        assert other.claim_thread_creation(**SCOPE, now_ms=4001) is None
        route = other.get_route(req.capture_id)
        assert (route.state, route.attempt_started_at) == ('creating', 4000)
        assert not store.db.in_transaction


@pytest.mark.parametrize('tamper', ['captures.transcript', 'relay_requests.transcript', 'relay_requests.digest', 'expired', 'unbound_card'])
def test_creation_requires_original_unexpired_bound_request(tmp_path, tamper):
    store, req = prepared(tmp_path)
    with store:
        if '.' in tamper:
            table, column = tamper.split('.')
            store.db.execute(f"UPDATE {table} SET {column}='tampered'")
        elif tamper == 'unbound_card':
            store.db.execute('UPDATE relay_requests SET message_id=NULL')
        assert store.claim_thread_creation(**SCOPE, now_ms=req.expires_at if tamper == 'expired' else 4000) is None
        assert store.get_route(req.capture_id).attempt_started_at is None


@pytest.mark.parametrize('stop', [None, 'expired', 'reset', 'uncertain', 'captures.transcript', 'relay_requests.digest'])
def test_remember_then_ready_revalidates_and_late_identity_is_inert(tmp_path, stop):
    store, req = prepared(tmp_path)
    with store:
        with pytest.raises(RuntimeError):
            store.remember_thread(req.capture_id, CARD)
        store.claim_thread_creation(**SCOPE, now_ms=4000)
        assert not store.ready_thread(req.capture_id, **SCOPE, now_ms=4001)
        if stop == 'expired':
            store.expire_due(**SCOPE, now_ms=req.expires_at)
        elif stop == 'reset':
            store.reset(**SCOPE)
        elif stop == 'uncertain':
            store.db.execute("UPDATE relay_threads SET state='uncertain',failure_code='interrupted'")
        elif stop:
            table, col = stop.split('.')
            store.db.execute(f"UPDATE {table} SET {col}='tampered'")
        store.remember_thread(req.capture_id, CARD)
        store.remember_thread(req.capture_id, CARD)
        assert store.get_route(req.capture_id).thread_id == CARD
        for field in SCOPE:
            assert not store.ready_thread(req.capture_id, **(SCOPE | {field: '923456789012345678'}), now_ms=4002)
        assert store.ready_thread(req.capture_id, **SCOPE, now_ms=4002) is (stop is None)
        assert not store.ready_thread(req.capture_id, **SCOPE, now_ms=4003)
        assert store.claim_thread_creation(**SCOPE, now_ms=4003) is None


@pytest.mark.parametrize('bad', ['', '123', 523456789012345678, '９' * 18, '623456789012345678'])
def test_remember_rejects_wrong_identity(tmp_path, bad):
    store, req = prepared(tmp_path)
    with store:
        store.claim_thread_creation(**SCOPE, now_ms=4000)
        with pytest.raises((ValueError, RuntimeError)):
            store.remember_thread(req.capture_id, bad)
        assert store.get_route(req.capture_id).thread_id is None


@pytest.mark.parametrize('uncertain', [False, True])
@pytest.mark.parametrize('request_state', ['approved_waiting', 'reset', 'expired', 'dispatching', 'turn_finished'])
def test_failure_is_terminal_and_duplicate_callbacks_do_not_rewrite(tmp_path, uncertain, request_state):
    store, req = prepared(tmp_path)
    with store:
        store.claim_thread_creation(**SCOPE, now_ms=4000)
        store.remember_thread(req.capture_id, CARD)
        store.db.execute('UPDATE relay_requests SET state=?', (request_state,))
        if request_state in ('dispatching', 'turn_finished'):
            store.db.execute("UPDATE relay_threads SET state='ready'")
        before = store.get_route(req.capture_id)
        store.fail_thread(req.capture_id, failure_code='timeout', uncertain=uncertain)
        route = store.get_route(req.capture_id)
        if request_state in ('dispatching', 'turn_finished'):
            assert route == before
        else:
            assert route.state == ('uncertain' if uncertain else 'failed')
            assert route.failure_code == 'timeout'
        assert route.thread_id == CARD
        expected = ('uncertain' if uncertain else 'failed') if request_state == 'approved_waiting' else request_state
        assert store.get(req.capture_id).state == expected
        store.fail_thread(req.capture_id, failure_code='storage', uncertain=not uncertain)
        assert store.get_route(req.capture_id) == route
        store.remember_thread(req.capture_id, CARD)
        assert not store.ready_thread(req.capture_id, **SCOPE, now_ms=5000)
        assert store.claim_thread_creation(**SCOPE, now_ms=5000) is None


@pytest.mark.parametrize('code,flag', [('raw remote text', False), ('timeout', 1), (None, True)])
def test_failure_rejects_nonfixed_values(tmp_path, code, flag):
    store, req = prepared(tmp_path)
    with store:
        with pytest.raises(ValueError):
            store.fail_thread(req.capture_id, failure_code=code, uncertain=flag)
        assert store.get_route(req.capture_id).state == 'unbound'


@pytest.mark.parametrize('stage', ['claim', 'remember', 'ready', 'fail'])
def test_creation_writes_rollback_when_commit_denied(tmp_path, stage):
    store, req = prepared(tmp_path)
    with store:
        if stage != 'claim':
            store.claim_thread_creation(**SCOPE, now_ms=4000)
        if stage in ('ready', 'fail'):
            store.remember_thread(req.capture_id, CARD)
        before = (store.get(req.capture_id), store.get_route(req.capture_id))
        def authorizer(action, arg1, arg2, db, trigger):
            return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and arg1 == 'COMMIT' else sqlite3.SQLITE_OK
        store.db.set_authorizer(authorizer)
        with pytest.raises(sqlite3.DatabaseError):
            if stage == 'claim':
                store.claim_thread_creation(**SCOPE, now_ms=4001)
            elif stage == 'remember':
                store.remember_thread(req.capture_id, CARD)
            elif stage == 'ready':
                store.ready_thread(req.capture_id, **SCOPE, now_ms=4001)
            else:
                store.fail_thread(req.capture_id, failure_code='transport', uncertain=True)
        store.db.set_authorizer(None)
        assert not store.db.in_transaction
        assert (store.get(req.capture_id), store.get_route(req.capture_id)) == before


def test_lock_and_ordered_claims_and_unique_identity(tmp_path):
    store, req = prepared(tmp_path)
    capture(tmp_path, text='Second synthetic request', now=2000)
    with store, RelayStore(tmp_path) as other:
        second = store.claim_card(**SCOPE, now_ms=2000, thread_mode=True)
        second = store.bind_card(second.capture_id, '623456789012345678')
        store.decide(**decision(second))
        other.db.execute('BEGIN IMMEDIATE')
        with pytest.raises(sqlite3.OperationalError, match='locked'):
            store.claim_thread_creation(**SCOPE, now_ms=4000)
        other.db.execute('ROLLBACK')
        assert store.get_route(req.capture_id).state == 'unbound'
        assert store.claim_thread_creation(**SCOPE, now_ms=4000).capture_id == req.capture_id
        assert other.claim_thread_creation(**SCOPE, now_ms=4000).capture_id == second.capture_id
        store.remember_thread(req.capture_id, CARD)
        with pytest.raises(sqlite3.IntegrityError):
            store.db.execute('UPDATE relay_threads SET thread_id=? WHERE capture_id=?', (CARD, second.capture_id))
        assert store.get_route(second.capture_id).thread_id is None


def test_prospective_route_is_atomic_and_legacy_dispatch_cannot_claim(tmp_path):
    capture(tmp_path)
    with RelayStore(tmp_path) as store:
        before = list(store.db.execute('PRAGMA table_info(captures)'))
        store.db.execute("CREATE TEMP TRIGGER reject_route BEFORE INSERT ON relay_threads BEGIN SELECT RAISE(ABORT, 'injected'); END")
        with pytest.raises(sqlite3.IntegrityError, match='injected'):
            store.claim_card(**SCOPE, now_ms=2000, thread_mode=True)
        assert store.db.execute('SELECT COUNT(*) FROM relay_requests').fetchone()[0] == 0
        store.db.execute('DROP TRIGGER reject_route')
        req = store.claim_card(**SCOPE, now_ms=2000, thread_mode=True)
        route = store.get_route(req.capture_id)
        assert (route.state, route.thread_id, route.attempt_started_at, route.failure_code) == ('unbound', None, None, None)
        with pytest.raises(FrozenInstanceError):
            route.state = 'ready'
        req = store.bind_card(req.capture_id, CARD)
        store.decide(**decision(req))
        assert store.claim_dispatch(**SCOPE, now_ms=4000) is None
        assert store.db.execute('PRAGMA user_version').fetchone()[0] == 2
        assert list(store.db.execute('PRAGMA table_info(captures)')) == before
        assert store.get_route(99999) is None


def creation_batch(tmp_path, kinds):
    """Real synthetic approvals; only expiry/integrity are invalidated afterward."""
    capture(tmp_path)
    store = RelayStore(tmp_path)
    requests = []
    for index, kind in enumerate(kinds):
        if index:
            capture(tmp_path, text=f'Synthetic batch request {index}', now=1000 + index)
        req = store.claim_card(**SCOPE, now_ms=2000, thread_mode=True)
        req = store.bind_card(req.capture_id, str(int(CARD) + index))
        assert store.decide(**decision(req)) == 'approved_waiting'
        if kind == 'expired':
            store.db.execute('UPDATE relay_requests SET expires_at=4000 WHERE capture_id=?', (req.capture_id,))
        elif kind == 'tampered':
            store.db.execute("UPDATE captures SET transcript='Synthetic tampering' WHERE id=?", (req.capture_id,))
        requests.append(store.get(req.capture_id))
    return store, requests


def observe_creation(store, monkeypatch):
    examined, statements = [], []
    authorize = store._thread_authorized

    def observing(req, **scope):
        examined.append(req.capture_id)
        return authorize(req, **scope)

    monkeypatch.setattr(store, '_thread_authorized', observing)
    store.db.set_trace_callback(statements.append)
    return examined, statements


def mutations(statements):
    return [sql for sql in statements if sql.lstrip().split()[0].upper() in {'UPDATE', 'INSERT', 'DELETE', 'REPLACE'}]


def test_creation_bounded_invalid_prefix_progresses_on_later_calls(tmp_path, monkeypatch):
    kinds = ['expired'] * 105 + ['tampered'] * 105 + ['valid']
    store, requests = creation_batch(tmp_path, kinds)
    with store:
        before_routes = [store.get_route(req.capture_id) for req in requests]
        examined, statements = observe_creation(store, monkeypatch)
        for start, stop in [(0, 100), (100, 200), (200, 211)]:
            examined.clear()
            statements.clear()
            claimed = store.claim_thread_creation(**SCOPE, now_ms=4000)
            assert len(examined) <= 100, f'examinations={len(examined)}, mutations={len(mutations(statements))}'
            assert examined == [req.capture_id for req in requests[start:stop]]
            assert len(mutations(statements)) == stop - start
            assert (claimed.capture_id if claimed else None) == (requests[-1].capture_id if stop == 211 else None)
            for index, req in enumerate(requests):
                current = store.get(req.capture_id)
                assert current.expires_at == req.expires_at
                if index >= stop:
                    assert current == req
                elif kinds[index] != 'valid':
                    assert current.state == ('expired' if kinds[index] == 'expired' else 'uncertain')
                route = store.get_route(req.capture_id)
                if claimed and req.capture_id == claimed.capture_id:
                    assert (route.state, route.attempt_started_at) == ('creating', 4000)
                else:
                    assert route == before_routes[index]
            assert not store.db.in_transaction
        examined.clear()
        statements.clear()
        assert store.claim_thread_creation(**SCOPE, now_ms=4001) is None
        assert examined == []
        assert mutations(statements) == []


@pytest.mark.parametrize('valid_index', [2, 99])
def test_creation_bounded_pass_stops_at_first_valid_candidate(tmp_path, monkeypatch, valid_index):
    store, requests = creation_batch(tmp_path, ['expired'] + ['tampered'] * (valid_index - 1) + ['valid', 'tampered', 'valid'])
    with store:
        before = [(store.get(req.capture_id), store.get_route(req.capture_id)) for req in requests]
        examined, statements = observe_creation(store, monkeypatch)
        claimed = store.claim_thread_creation(**SCOPE, now_ms=4000)
        assert claimed.capture_id == requests[valid_index].capture_id
        assert examined == [req.capture_id for req in requests[:valid_index + 1]]
        assert len(mutations(statements)) == valid_index + 1
        for index, req in enumerate(requests):
            route = store.get_route(req.capture_id)
            assert store.get(req.capture_id).expires_at == req.expires_at
            if index == valid_index:
                assert (route.state, route.attempt_started_at) == ('creating', 4000)
            else:
                assert route == before[index][1]
            if index > valid_index:
                assert (store.get(req.capture_id), route) == before[index]
        assert not store.db.in_transaction


@pytest.mark.parametrize('selected', [False, True])
def test_creation_bounded_batch_commit_denial_rolls_back_every_row(tmp_path, monkeypatch, selected):
    kinds = ['expired'] * 49 + ['tampered'] * 50 + ['valid' if selected else 'tampered'] + ['valid']
    store, requests = creation_batch(tmp_path, kinds)
    with store:
        before = [(store.get(req.capture_id), store.get_route(req.capture_id)) for req in requests]
        examined, statements = observe_creation(store, monkeypatch)

        def deny_commit(action, arg1, arg2, db, trigger):
            return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and arg1 == 'COMMIT' else sqlite3.SQLITE_OK

        store.db.set_authorizer(deny_commit)
        try:
            with pytest.raises(sqlite3.DatabaseError):
                store.claim_thread_creation(**SCOPE, now_ms=4000)
        finally:
            store.db.set_authorizer(None)
        assert examined == [req.capture_id for req in requests[:100]]
        assert len(mutations(statements)) == 100
        assert not store.db.in_transaction
        assert [(store.get(req.capture_id), store.get_route(req.capture_id)) for req in requests] == before


@pytest.mark.parametrize('bad', [0, 1, None, 'true'])
def test_thread_mode_requires_exact_bool(tmp_path, bad):
    capture(tmp_path)
    with RelayStore(tmp_path) as store:
        with pytest.raises(ValueError):
            store.claim_card(**SCOPE, now_ms=2000, thread_mode=bad)
        req = store.claim_card(**SCOPE, now_ms=2000)
        assert store.get_route(req.capture_id) is None


COOLDOWN_CODES = ['route_cooldown', 'global_cooldown', 'http_429_route', 'http_429_shared', 'http_429_global']
OLD_ROUTE_SQL = """CREATE TABLE relay_threads (
    capture_id INTEGER PRIMARY KEY REFERENCES relay_requests(capture_id),
    thread_id TEXT UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('unbound','creating','ready','failed','uncertain')),
    attempt_started_at INTEGER, failure_code TEXT)"""


def defer(store, req, *, now=4001, due=8000, code='route_cooldown'):
    return store.defer_thread(req.capture_id, expected_request=req, **SCOPE,
                             now_ms=now, retry_not_before=due, failure_code=code)


@pytest.mark.parametrize('code', COOLDOWN_CODES)
@pytest.mark.parametrize('known', [False, True])
def test_safe_wait_survives_restart_and_due_claim_preserves_attempt(tmp_path, code, known):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        if known:
            store.remember_thread(req.capture_id, CARD)
        assert defer(store, req, code=code)
        route = store.get_route(req.capture_id)
        assert (route.state, route.attempt_started_at, route.retry_not_before, route.failure_code) == ('rate_wait', 4000, 8000, code)
        assert store.get(req.capture_id) == req
        assert not defer(store, req, due=9000)
        assert store.get_route(req.capture_id) == route
    with RelayStore(tmp_path) as store, RelayStore(tmp_path) as other:
        store.recover(**SCOPE, now_ms=5000)
        assert store.get_route(req.capture_id) == route
        assert store.claim_thread_creation(**SCOPE, now_ms=7999) is None
        assert store.claim_thread_creation(**SCOPE, now_ms=8000) == req
        assert other.claim_thread_creation(**SCOPE, now_ms=8000) is None
        claimed = store.get_route(req.capture_id)
        assert (claimed.state, claimed.attempt_started_at, claimed.retry_not_before) == ('creating', 4000, None)
        assert claimed.thread_id == (CARD if known else None)
        assert store.authorize_thread_creation(req.capture_id, expected_request=req, **SCOPE, now_ms=8000) is (not known)
        if not known:
            store.remember_thread(req.capture_id, CARD)
        assert store.ready_thread(req.capture_id, expected_request=req, **SCOPE, now_ms=8001)
        assert store.get_route(req.capture_id).failure_code is None
        assert store.get_route(req.capture_id).retry_not_before is None


def test_final_readback_wait_remains_ready_and_dispatch_is_due_once(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        ready(store, req)
        req = store.get(req.capture_id)
        assert defer(store, req, now=5000)
        assert store.get_route(req.capture_id).state == 'ready'
        assert store.thread_candidates(**SCOPE, now_ms=7999) == []
        assert store.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=7999) is None
    with RelayStore(tmp_path) as store:
        store.recover(**SCOPE, now_ms=7000)
        assert store.claim_thread_creation(**SCOPE, now_ms=9000) is None
        assert store.thread_candidates(**SCOPE, now_ms=8000) == [req]
        assert store.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=8000).state == 'dispatching'
        assert store.claim_thread_dispatch(req.capture_id, **SCOPE, now_ms=8001) is None


@pytest.mark.parametrize('operation', ['defer', 'authorize'])
@pytest.mark.parametrize('tamper', ['reset', 'expired', 'rejected', 'captures.transcript', 'relay_requests.transcript',
                                    'relay_requests.digest', 'relay_requests.nonce', 'relay_requests.message_id', 'scope'])
def test_wait_authority_requires_current_exact_approval(tmp_path, operation, tamper):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        scope = SCOPE.copy()
        now = 4001
        if tamper == 'reset':
            store.reset(**SCOPE)
        elif tamper == 'expired':
            now = req.expires_at
        elif tamper == 'rejected':
            store.db.execute("UPDATE relay_requests SET state='rejected'")
        elif tamper == 'scope':
            scope['approver_id'] = '923456789012345678'
        else:
            table, col = tamper.split('.')
            store.db.execute(f"UPDATE {table} SET {col}='synthetic-change'")
        before_route = store.get_route(req.capture_id)
        kwargs = dict(expected_request=req, **scope, now_ms=now)
        if operation == 'defer':
            assert not store.defer_thread(req.capture_id, **kwargs, retry_not_before=now + 1000, failure_code='route_cooldown')
        else:
            assert not store.authorize_thread_creation(req.capture_id, **kwargs)
        if operation == 'defer' and tamper in ('reset', 'expired', 'rejected'):
            route = store.get_route(req.capture_id)
            assert route.state == 'rate_wait'
            assert route.retry_not_before == now + 1000
            assert route.failure_code == 'route_cooldown'
            assert route.attempt_started_at == before_route.attempt_started_at
        else:
            assert store.get_route(req.capture_id) == before_route


@pytest.mark.parametrize('due', [None, True, 0, -1, 4001, 4000, 8000.0, float('inf'), float('nan'), '8000', 2**63])
def test_defer_rejects_invalid_due_without_mutation(tmp_path, due):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        before = store.get_route(req.capture_id)
        with pytest.raises(ValueError):
            defer(store, req, due=due)
        assert store.get_route(req.capture_id) == before


@pytest.mark.parametrize('code', [None, 'rate_limited', 'timeout', 'raw remote text', True])
def test_defer_rejects_nonclassified_provenance(tmp_path, code):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        with pytest.raises(ValueError):
            defer(store, req, code=code)
        assert store.get_route(req.capture_id).state == 'creating'


@pytest.mark.parametrize('stage', ['unbound', 'failed', 'uncertain', 'rate_wait', 'ready'])
def test_creation_authorization_never_uses_other_route_stages(tmp_path, stage):
    store, req = prepared(tmp_path)
    with store:
        if stage != 'unbound':
            req = store.claim_thread_creation(**SCOPE, now_ms=4000)
            if stage in ('failed', 'uncertain'):
                store.fail_thread(req.capture_id, failure_code='rate_limited', uncertain=stage == 'uncertain')
            elif stage == 'rate_wait':
                assert defer(store, req)
            else:
                store.remember_thread(req.capture_id, CARD)
                store.ready_thread(req.capture_id, **SCOPE, now_ms=4001)
        assert not store.authorize_thread_creation(req.capture_id, expected_request=req, **SCOPE, now_ms=9000)
        if stage in ('failed', 'uncertain'):
            assert not defer(store, req)
            assert store.claim_thread_creation(**SCOPE, now_ms=9000) is None


@pytest.mark.parametrize('tamper', ["failure_code='rate_limited'", 'retry_not_before=NULL', "retry_not_before='invalid'",
                                    'attempt_started_at=NULL', "thread_id='623456789012345678'"])
def test_due_wait_requires_saved_safe_provenance(tmp_path, tamper):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        assert defer(store, req)
        store.db.execute(f'UPDATE relay_threads SET {tamper}')
        assert store.claim_thread_creation(**SCOPE, now_ms=9000) is None


@pytest.mark.parametrize('stop', ['reset', 'expired', 'tampered'])
def test_waiting_request_cannot_resume_after_lost_authority(tmp_path, stop):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        assert defer(store, req, due=req.expires_at + 1000 if stop == 'expired' else 8000)
        if stop == 'reset':
            store.reset_thread(CARD, **SCOPE)
        elif stop == 'tampered':
            store.db.execute("UPDATE captures SET transcript='synthetic-change'")
        now = req.expires_at if stop == 'expired' else 9000
        store.expire_due(**SCOPE, now_ms=now)
        assert store.claim_thread_creation(**SCOPE, now_ms=now) is None
        assert store.get(req.capture_id).state == {'reset': 'reset', 'expired': 'expired', 'tampered': 'uncertain'}[stop]


def old_route_fixture(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        ready(store, req)
        store.reset_thread(CARD, **SCOPE, reset_message_id='723456789012345678')
        row = tuple(store.db.execute('SELECT capture_id,thread_id,state,attempt_started_at,failure_code FROM relay_threads').fetchone())
        store.db.execute('DROP TABLE relay_threads')
        store.db.execute(OLD_ROUTE_SQL)
        store.db.execute('INSERT INTO relay_threads VALUES(?,?,?,?,?)', row)
    return req, row


def test_exact_old_route_schema_migrates_atomically_and_second_open_is_noop(tmp_path):
    req, before = old_route_fixture(tmp_path)
    with RelayStore(tmp_path) as store:
        assert tuple(store.db.execute('SELECT * FROM relay_threads').fetchone()) == before + (None,)
        assert store.db.execute('PRAGMA foreign_keys').fetchone()[0] == 1
        assert store.db.execute('PRAGMA foreign_key_check').fetchall() == []
        assert store.db.execute('PRAGMA user_version').fetchone()[0] == 2
        assert store.thread_reset_message(CARD, **SCOPE) == '723456789012345678'
        version = store.db.execute('PRAGMA schema_version').fetchone()[0]
        snapshot = list(store.db.iterdump())
    with RelayStore(tmp_path) as store:
        assert store.db.execute('PRAGMA schema_version').fetchone()[0] == version
        assert list(store.db.iterdump()) == snapshot


@pytest.mark.parametrize('extra', [
    'CREATE INDEX custom_route ON relay_threads(state)',
    'CREATE UNIQUE INDEX custom_unique_route ON relay_threads(failure_code)',
    'CREATE TRIGGER custom_route AFTER UPDATE ON relay_threads BEGIN SELECT 1; END',
])
def test_migration_rejects_unrecognized_auxiliary_schema_without_changes(tmp_path, extra):
    old_route_fixture(tmp_path)
    with sqlite3.connect(tmp_path / 'bridge.sqlite3') as db:
        db.execute(extra)
        before = list(db.iterdump())
    with pytest.raises(RuntimeError, match='schema'):
        RelayStore(tmp_path)
    with sqlite3.connect(tmp_path / 'bridge.sqlite3') as db:
        assert list(db.iterdump()) == before


@pytest.mark.parametrize('failure', ['commit', 'copy'])
def test_route_migration_rolls_back_every_schema_and_row_change(tmp_path, monkeypatch, failure):
    old_route_fixture(tmp_path)
    real_connect = sqlite3.connect
    with real_connect(tmp_path / 'bridge.sqlite3') as db:
        before = list(db.iterdump())
    def connect(*args, **kwargs):
        db = real_connect(*args, **kwargs)
        def authorize(action, arg1, arg2, *_):
            if failure == 'commit' and action == sqlite3.SQLITE_TRANSACTION and arg1 == 'COMMIT':
                return sqlite3.SQLITE_DENY
            if failure == 'copy' and action == sqlite3.SQLITE_INSERT and arg1 == 'relay_threads_cooldown':
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        db.set_authorizer(authorize)
        return db
    monkeypatch.setattr(sqlite3, 'connect', connect)
    with pytest.raises(sqlite3.DatabaseError):
        RelayStore(tmp_path)
    with real_connect(tmp_path / 'bridge.sqlite3') as db:
        assert list(db.iterdump()) == before


def test_deferral_commit_failure_rolls_back_and_propagates(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        before = (store.get(req.capture_id), store.get_route(req.capture_id))
        store.db.set_authorizer(lambda action, arg1, *_: sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and arg1 == 'COMMIT' else sqlite3.SQLITE_OK)
        try:
            with pytest.raises(sqlite3.DatabaseError):
                defer(store, req)
        finally:
            store.db.set_authorizer(None)
        assert not store.db.in_transaction
        assert (store.get(req.capture_id), store.get_route(req.capture_id)) == before


@pytest.mark.parametrize('code', ['global_cooldown', 'http_429_global'])
@pytest.mark.parametrize('owner_state', ['approved_waiting', 'reset', 'expired'])
def test_saved_global_cooldown_blocks_siblings_after_restart(tmp_path, code, owner_state):
    store, requests = creation_batch(tmp_path, ['valid', 'valid', 'valid'])
    owner, final, fresh = requests
    with store:
        owner = store.claim_thread_creation(**SCOPE, now_ms=4000)
        ready(store, final)
        assert defer(store, owner, code=code)
        store.db.execute('UPDATE relay_requests SET state=? WHERE capture_id=?', (owner_state, owner.capture_id))
    with RelayStore(tmp_path) as store:
        store.recover(**SCOPE, now_ms=5000)
        assert store.thread_global_retry_not_before(**SCOPE, now_ms=5000) == 8000
        assert store.claim_thread_creation(**SCOPE, now_ms=5000) is None
        assert store.thread_candidates(**SCOPE, now_ms=5000) == []
        assert store.claim_thread_dispatch(final.capture_id, **SCOPE, now_ms=5000) is None
        assert store.thread_global_retry_not_before(**SCOPE, now_ms=8000) is None
        claim = store.claim_thread_creation(**SCOPE, now_ms=8000)
        assert claim.capture_id == (owner.capture_id if owner_state == 'approved_waiting' else fresh.capture_id)
        assert store.thread_candidates(**SCOPE, now_ms=8000) == [final]


def test_global_wait_rechecks_post_admission_and_route_wait_is_not_global(tmp_path):
    store, requests = creation_batch(tmp_path, ['valid', 'valid'])
    with store:
        first = store.claim_thread_creation(**SCOPE, now_ms=4000)
        second = store.claim_thread_creation(**SCOPE, now_ms=4000)
        assert defer(store, first, code='http_429_route')
        assert store.authorize_thread_creation(second.capture_id, expected_request=second, **SCOPE, now_ms=5000)
        store.db.execute("UPDATE relay_threads SET failure_code='http_429_global' WHERE capture_id=?", (first.capture_id,))
        assert not store.authorize_thread_creation(second.capture_id, expected_request=second, **SCOPE, now_ms=5000)
        assert store.authorize_thread_creation(second.capture_id, expected_request=second, **SCOPE, now_ms=8000)


def test_migration_does_not_adopt_a_preexisting_rebuild_table(tmp_path):
    old_route_fixture(tmp_path)
    with sqlite3.connect(tmp_path / 'bridge.sqlite3') as db:
        db.execute(OLD_ROUTE_SQL.replace('relay_threads (', 'relay_threads_cooldown (').replace("'unbound','creating'", "'unbound','rate_wait','creating'").replace('failure_code TEXT', 'failure_code TEXT, retry_not_before INTEGER'))
        before = list(db.iterdump())
    with pytest.raises(sqlite3.DatabaseError):
        RelayStore(tmp_path)
    with sqlite3.connect(tmp_path / 'bridge.sqlite3') as db:
        assert list(db.iterdump()) == before


def test_migration_preserves_every_legacy_route_state(tmp_path):
    states = ['unbound', 'creating', 'ready', 'failed', 'uncertain']
    store, requests = creation_batch(tmp_path, ['valid'] * len(states))
    with store:
        for state, req in zip(states, requests):
            if state != 'unbound':
                assert store.claim_thread_creation(**SCOPE, now_ms=4000).capture_id == req.capture_id
                store.remember_thread(req.capture_id, req.message_id)
                if state == 'ready':
                    assert store.ready_thread(req.capture_id, **SCOPE, now_ms=4001)
                elif state in ('failed', 'uncertain'):
                    store.fail_thread(req.capture_id, failure_code='rate_limited', uncertain=state == 'uncertain')
            else:
                # Keep this row virgin while later real claims are made.
                store.db.execute("UPDATE relay_requests SET state='pending' WHERE capture_id=?", (req.capture_id,))
        rows = [tuple(row) for row in store.db.execute('SELECT capture_id,thread_id,state,attempt_started_at,failure_code FROM relay_threads ORDER BY capture_id')]
        before_requests = [tuple(row) for row in store.db.execute('SELECT * FROM relay_requests ORDER BY capture_id')]
        store.db.execute('DROP TABLE relay_threads')
        store.db.execute(OLD_ROUTE_SQL)
        store.db.executemany('INSERT INTO relay_threads VALUES(?,?,?,?,?)', rows)
    with RelayStore(tmp_path) as store:
        assert [tuple(row) for row in store.db.execute('SELECT * FROM relay_threads ORDER BY capture_id')] == [row + (None,) for row in rows]
        assert [tuple(row) for row in store.db.execute('SELECT * FROM relay_requests ORDER BY capture_id')] == before_requests
        assert store.claim_thread_creation(**SCOPE, now_ms=9000) is None


@pytest.mark.parametrize('revoked', ['reset', 'expired', 'rejected'])
@pytest.mark.parametrize('stage', ['creating', 'ready'])
def test_late_global_denial_preserves_cooldown_without_reviving_approval(tmp_path, revoked, stage):
    store, requests = creation_batch(tmp_path, ['valid', 'valid'])
    owner, sibling = requests
    with store:
        owner = store.claim_thread_creation(**SCOPE, now_ms=4000)
        if stage == 'ready':
            store.remember_thread(owner.capture_id, owner.message_id)
            assert store.ready_thread(owner.capture_id, **SCOPE, now_ms=4001)
        if revoked == 'reset':
            store.reset_thread(owner.message_id, **SCOPE)
        else:
            store.db.execute('UPDATE relay_requests SET state=? WHERE capture_id=?', (revoked, owner.capture_id))
        assert not defer(store, owner, now=5000, code='http_429_global')
        route = store.get_route(owner.capture_id)
        assert route.state == ('rate_wait' if stage == 'creating' else 'ready')
        assert (route.retry_not_before, route.failure_code, route.attempt_started_at) == (8000, 'http_429_global', 4000)
        assert store.get(owner.capture_id).state == revoked
    with RelayStore(tmp_path) as store:
        store.recover(**SCOPE, now_ms=5001)
        assert store.get_route(owner.capture_id) == route
        assert store.get(owner.capture_id).state == revoked
        assert store.claim_thread_creation(**SCOPE, now_ms=7999) is None
        assert store.thread_global_retry_not_before(**SCOPE, now_ms=7999) == 8000
        assert store.claim_thread_dispatch(owner.capture_id, **SCOPE, now_ms=8000) is None
        assert store.claim_thread_creation(**SCOPE, now_ms=8000) == sibling


@pytest.mark.parametrize('tamper', ['captures.transcript', 'relay_requests.digest', 'relay_requests.nonce', 'scope'])
def test_revoked_cooldown_metadata_still_requires_exact_original_identity(tmp_path, tamper):
    store, req = prepared(tmp_path)
    with store:
        req = store.claim_thread_creation(**SCOPE, now_ms=4000)
        store.reset_thread(req.message_id, **SCOPE)
        scope = SCOPE.copy()
        if tamper == 'scope':
            scope['approver_id'] = '923456789012345678'
        else:
            table, col = tamper.split('.')
            store.db.execute(f"UPDATE {table} SET {col}='synthetic-change'")
        before = store.get_route(req.capture_id)
        assert not store.defer_thread(req.capture_id, expected_request=req, **scope,
            now_ms=4001, retry_not_before=8000, failure_code='http_429_global')
        assert store.get_route(req.capture_id) == before


@pytest.mark.parametrize('action', ['CASCADE', 'SET NULL', 'NO ACTION'])
@pytest.mark.parametrize('target', ['relay_threads', 'RELAY_THREADS'])
def test_old_route_migration_rejects_inbound_foreign_keys_without_data_loss(tmp_path, action, target):
    req, _ = old_route_fixture(tmp_path)
    with sqlite3.connect(tmp_path / 'bridge.sqlite3') as db:
        db.execute('PRAGMA foreign_keys=ON')
        db.execute(f'''CREATE TABLE "synthetic child""table" (
            id INTEGER PRIMARY KEY, route_id INTEGER REFERENCES "{target}"(capture_id)
            ON DELETE {action})''')
        db.execute('INSERT INTO "synthetic child""table" VALUES(1,?)', (req.capture_id,))
        before = list(db.iterdump())
    error = None
    try:
        with RelayStore(tmp_path):
            pass
    except RuntimeError as caught:
        error = caught
    with sqlite3.connect(tmp_path / 'bridge.sqlite3') as db:
        assert db.execute('SELECT route_id FROM "synthetic child""table"').fetchone() == (req.capture_id,)
        assert list(db.iterdump()) == before
        assert db.execute("SELECT name FROM sqlite_master WHERE name='relay_threads_cooldown'").fetchall() == []
    assert error is not None and 'schema' in str(error)
