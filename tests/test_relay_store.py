from pathlib import Path

import pytest

from pebble_bridge.repository import Repository


SCOPE = dict(guild_id='323456789012345678', channel_id='223456789012345678',
             approver_id='123456789012345678')


def capture(tmp_path, text='Synthetic exact request', now=1000):
    repo = Repository(tmp_path)
    item, _ = repo.persist_capture(client='synthetic', recorded_at=now, received_at=now,
                                  transcript=text, audio=b'synthetic-recording', forced_new_topic=True)
    repo.connection.close()
    return item


def test_claim_card_has_fixed_bound_authority_and_no_second_claim(tmp_path):
    import importlib.util
    assert importlib.util.find_spec('pebble_bridge.relay_store') is not None, 'shared request store not implemented'
    from pebble_bridge import relay_store
    item = capture(tmp_path)
    with relay_store.RelayStore(tmp_path) as store:
        request = store.claim_card(**SCOPE, now_ms=2000)
        assert request.capture_id == item.id
        assert request.transcript == item.transcript
        assert request.state == 'posting'
        assert request.expires_at == 1802000
        assert request.audio_expires_at == 604801000
        assert len(request.nonce) >= 32
        assert store.claim_card(**SCOPE, now_ms=4000) is None
        bound = store.bind_card(item.id, '523456789012345678')
        assert bound.state == 'pending'
        assert bound.message_id == '523456789012345678'
        assert bound.expires_at == request.expires_at
        assert Path(item.audio_path).read_bytes() == b'synthetic-recording'


def prepared(tmp_path):
    from pebble_bridge.relay_store import RelayStore
    capture(tmp_path)
    store = RelayStore(tmp_path)
    req = store.claim_card(**SCOPE, now_ms=2000)
    return store, store.bind_card(req.capture_id, '523456789012345678')


def decision(req, **overrides):
    return dict(capture_id=req.capture_id, actor_id=req.approver_id, guild_id=req.guild_id,
                channel_id=req.channel_id, message_id=req.message_id, nonce=req.nonce,
                digest=req.digest, now_ms=3000, action='approve') | overrides


@pytest.mark.parametrize('field,value', [
    ('actor_id', '923456789012345678'), ('guild_id', '923456789012345678'),
    ('channel_id', '923456789012345678'), ('message_id', '923456789012345678'),
    ('nonce', 'wrong'), ('digest', 'wrong'), ('action', 'unexpected')])
def test_decision_is_capture_bound_and_single_use(tmp_path, field, value):
    store, req = prepared(tmp_path)
    with store:
        assert hasattr(store, 'decide'), 'authenticated decision transition missing'
        assert store.decide(**decision(req, **{field: value})) == 'invalid'
        assert store.get(req.capture_id).state == 'pending'
        assert store.decide(**decision(req)) == 'approved_waiting'
        assert store.decide(**decision(req)) == 'already_decided'
        assert store.get(req.capture_id).state == 'approved_waiting'


def test_rejection_is_terminal_without_execution(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        assert store.decide(**decision(req, action='reject')) == 'rejected', 'reject must be terminal'
        assert store.decide(**decision(req)) == 'already_decided'
        assert store.get(req.capture_id).state == 'rejected'


def test_decision_at_deadline_expires_without_renewal(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        assert store.decide(**decision(req, now_ms=req.expires_at)) == 'expired', 'deadline ignored'
        assert store.get(req.capture_id).expires_at == req.expires_at
        assert store.decide(**decision(req)) == 'already_decided'


def test_mutated_capture_cannot_execute_original_approval(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        store.db.execute("UPDATE captures SET transcript='Changed instruction' WHERE id=?", (req.capture_id,))
        assert store.decide(**decision(req)) == 'uncertain', 'changed instruction accepted'
        assert store.get(req.capture_id).state == 'uncertain'


def test_dispatch_claim_is_scoped_single_use_and_finishes_truthfully(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        store.decide(**decision(req))
        assert hasattr(store, 'claim_dispatch'), 'durable dispatch claim missing'
        assert store.claim_dispatch(**(SCOPE | {'channel_id':'923456789012345678'}), now_ms=4000) is None
        claimed = store.claim_dispatch(**SCOPE, now_ms=4000)
        assert claimed.capture_id == req.capture_id and claimed.state == 'dispatching'
        assert store.claim_dispatch(**SCOPE, now_ms=4000) is None
        store.finish(req.capture_id)
        assert store.get(req.capture_id).state == 'turn_finished'
        with pytest.raises(RuntimeError):
            store.finish(req.capture_id)


def test_queued_approval_expires_before_dispatch(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        store.decide(**decision(req))
        assert store.claim_dispatch(**SCOPE, now_ms=req.expires_at) is None, 'queue extended approval lifetime'
        assert store.get(req.capture_id).state == 'expired'


@pytest.mark.parametrize('approve_first', [False, True])
def test_reset_revokes_unstarted_authority(tmp_path, approve_first):
    store, req = prepared(tmp_path)
    with store:
        if approve_first:
            store.decide(**decision(req))
        assert hasattr(store, 'reset'), 'queued reset invalidation missing'
        store.reset(**SCOPE)
        assert store.get(req.capture_id).state == 'reset'
        assert store.claim_dispatch(**SCOPE, now_ms=4000) is None
        assert store.decide(**decision(req)) == 'already_decided'


@pytest.mark.parametrize('stage,expected', [('posting','uncertain'), ('dispatching','uncertain'),
                                           ('approved_waiting','approved_waiting')])
def test_restart_never_replays_ambiguous_started_work(tmp_path, stage, expected):
    from pebble_bridge.relay_store import RelayStore
    store, req = prepared(tmp_path)
    with store:
        store.db.execute('UPDATE relay_requests SET state=? WHERE capture_id=?', (stage,req.capture_id))
    with RelayStore(tmp_path) as restored:
        assert hasattr(restored, 'recover'), 'restart recovery missing'
        restored.recover(**SCOPE, now_ms=4000)
        assert restored.get(req.capture_id).state == expected
        assert restored.get(req.capture_id).expires_at == req.expires_at
        assert bool(restored.claim_dispatch(**SCOPE, now_ms=4000)) == (stage == 'approved_waiting')


def test_mutation_after_approval_is_rechecked_before_dispatch(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        store.decide(**decision(req))
        store.db.execute("UPDATE captures SET transcript='changed after approval' WHERE id=?", (req.capture_id,))
        assert store.claim_dispatch(**SCOPE, now_ms=4000) is None, 'dispatch failed to recheck immutable capture'
        assert store.get(req.capture_id).state == 'uncertain'


@pytest.mark.parametrize('column,value', [('discord_uploaded',1), ('thread_id','123'),
    ('hermes_session_id','legacy'), ('answer_text','legacy'), ('capture_post_token','legacy'),
    ('answer_post_token','legacy')])
def test_legacy_external_effect_captures_are_never_replayed(tmp_path,column,value):
    from pebble_bridge.relay_store import RelayStore
    item = capture(tmp_path)
    with RelayStore(tmp_path) as store:
        store.db.execute(f'UPDATE captures SET {column}=? WHERE id=?', (value,item.id))
        assert store.claim_card(**SCOPE, now_ms=2000) is None, 'legacy capture was replayed'


def test_unknown_bridge_schema_is_refused_without_migration(tmp_path):
    import sqlite3
    from pebble_bridge.relay_store import RelayStore
    capture(tmp_path)
    with sqlite3.connect(tmp_path / 'bridge.sqlite3') as db:
        db.execute('PRAGMA user_version=999')
    with pytest.raises(RuntimeError, match='schema'):
        RelayStore(tmp_path)


@pytest.mark.parametrize('bad', ['', 'abc', '１２３４５６７８９０１２３４５６７８', '123'])
def test_card_claim_requires_exact_numeric_scope(tmp_path, bad):
    from pebble_bridge.relay_store import RelayStore
    capture(tmp_path)
    with RelayStore(tmp_path) as store:
        with pytest.raises(ValueError, match='scope'):
            store.claim_card(**(SCOPE | {'approver_id':bad}), now_ms=2000)
        assert store.claim_card(**SCOPE, now_ms=2000) is not None


def test_database_lock_fails_closed_without_partial_decision(tmp_path):
    import sqlite3
    store, req = prepared(tmp_path)
    with store, sqlite3.connect(tmp_path / 'bridge.sqlite3', isolation_level=None) as other:
        other.execute('BEGIN IMMEDIATE')
        with pytest.raises(sqlite3.OperationalError, match='locked'):
            store.decide(**decision(req))
        other.execute('ROLLBACK')
        assert store.get(req.capture_id).state == 'pending'
        assert store.decide(**decision(req)) == 'approved_waiting'


def test_retained_audio_scope_is_independent_of_execution_expiry(tmp_path):
    store, req = prepared(tmp_path)
    with store:
        store.decide(**decision(req, now_ms=req.expires_at))
        assert hasattr(store, 'audio_record'), 'scoped retained audio lookup missing'
        params = dict(capture_id=req.capture_id, actor_id=req.approver_id,
                      guild_id=req.guild_id, channel_id=req.channel_id, now_ms=req.expires_at+1)
        record = store.audio_record(**params)
        assert Path(record['audio_path']).read_bytes() == b'synthetic-recording'
        assert 'transcript' not in record
        with pytest.raises(PermissionError):
            store.audio_record(**(params | {'actor_id':'923456789012345678'}))
        with pytest.raises(PermissionError):
            store.audio_record(**(params | {'now_ms':req.audio_expires_at}))
        assert store.get(req.capture_id).state == 'expired'


def test_audio_grant_is_turn_bound_single_use_not_execution_approval(tmp_path):
    store, req = prepared(tmp_path)
    who = dict(actor_id=req.approver_id, guild_id=req.guild_id, chat_id=req.channel_id,
               message_id='623456789012345678', profile='default')
    with store:
        assert hasattr(store, 'grant_audio'), 'turn-bound audio grant missing'
        with pytest.raises(PermissionError):
            store.claim_audio(req.capture_id, **who, kind='retrieve', now_ms=4000)
        store.grant_audio(req.capture_id, **who, capture_channel_id=req.channel_id,
                          kind='retrieve', now_ms=4000)
        with pytest.raises(PermissionError):
            store.claim_audio(req.capture_id, **(who | {'message_id':'723456789012345678'}),
                              kind='retrieve', now_ms=4000)
        result = store.claim_audio(req.capture_id, **who, kind='retrieve', now_ms=4000)
        assert result['audio_size'] == len(b'synthetic-recording')
        with pytest.raises(PermissionError):
            store.claim_audio(req.capture_id, **who, kind='retrieve', now_ms=4000)
        assert store.get(req.capture_id).state == 'pending'

def test_reset_consumes_audio_grants_without_deleting_retained_recording(tmp_path):
    store, req = prepared(tmp_path)
    who = dict(actor_id=req.approver_id, guild_id=req.guild_id, chat_id=req.channel_id,
               message_id='623456789012345678', profile='default')
    with store:
        store.grant_audio(req.capture_id, **who, capture_channel_id=req.channel_id,
                          kind='retrieve', now_ms=4000)
        store.reset(**SCOPE)
        # Replayed pre-reset events must not recreate the authority either.
        store.grant_audio(req.capture_id, **who, capture_channel_id=req.channel_id,
                          kind='retrieve', now_ms=4100)
        with pytest.raises(PermissionError):
            store.claim_audio(req.capture_id, **who, kind='retrieve', now_ms=4100)
        fresh = who | {'message_id': '723456789012345678'}
        store.grant_audio(req.capture_id, **fresh, capture_channel_id=req.channel_id,
                          kind='retrieve', now_ms=4200)
        assert store.claim_audio(req.capture_id, **fresh, kind='retrieve', now_ms=4200)

# NEXT_TEST
