"""Shared SQLite authority for the successor; no Discord/model/file-content I/O."""
from __future__ import annotations

import hashlib
import secrets
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterator

APPROVAL_MS = 30 * 60 * 1000
AUDIO_MS = 7 * 24 * 60 * 60 * 1000
COOLDOWN_CODES = frozenset({'route_cooldown', 'global_cooldown', 'http_429_route',
                            'http_429_shared', 'http_429_global'})


@dataclass(frozen=True)
class Request:
    capture_id: int
    guild_id: str
    channel_id: str
    approver_id: str
    transcript: str
    digest: str
    nonce: str
    expires_at: int
    audio_expires_at: int
    state: str
    message_id: str | None


@dataclass(frozen=True)
class Route:
    capture_id: int
    thread_id: str | None
    state: str
    attempt_started_at: int | None
    failure_code: str | None
    retry_not_before: int | None = None


class RelayStore:
    def __init__(self, data_dir: Path, *, audio_retention_ms: int = AUDIO_MS):
        self.data_dir = Path(data_dir)
        self.audio_retention_ms = audio_retention_ms
        path = self.data_dir / 'bridge.sqlite3'
        self.db = sqlite3.connect(f'{path.as_uri()}?mode=rw', uri=True,
                                  isolation_level=None, timeout=0.075)
        self.db.row_factory = sqlite3.Row
        if self.db.execute('PRAGMA user_version').fetchone()[0] != 2:
            self.db.close()
            raise RuntimeError('unsupported bridge schema')
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('''CREATE TABLE IF NOT EXISTS relay_requests (
            capture_id INTEGER PRIMARY KEY REFERENCES captures(id),
            guild_id TEXT NOT NULL, channel_id TEXT NOT NULL, approver_id TEXT NOT NULL,
            transcript TEXT NOT NULL, digest TEXT NOT NULL, nonce TEXT NOT NULL,
            expires_at INTEGER NOT NULL, audio_expires_at INTEGER NOT NULL,
            state TEXT NOT NULL, message_id TEXT UNIQUE
        )''')
        self.db.execute('''CREATE TABLE IF NOT EXISTS relay_audio_grants (
            capture_id INTEGER NOT NULL REFERENCES captures(id), actor_id TEXT NOT NULL,
            guild_id TEXT NOT NULL, chat_id TEXT NOT NULL, message_id TEXT NOT NULL,
            profile TEXT NOT NULL, kind TEXT NOT NULL, capture_channel_id TEXT NOT NULL,
            expires_at INTEGER NOT NULL, used INTEGER NOT NULL,
            PRIMARY KEY(message_id,kind)
        )''')

        old_route_sql = """CREATE TABLE IF NOT EXISTS relay_threads (
            capture_id INTEGER PRIMARY KEY REFERENCES relay_requests(capture_id),
            thread_id TEXT UNIQUE,
            state TEXT NOT NULL CHECK(state IN ('unbound','creating','ready','failed','uncertain')),
            attempt_started_at INTEGER, failure_code TEXT
        )"""
        route_sql = old_route_sql.replace("'unbound','creating'", "'unbound','rate_wait','creating'").replace(
            'failure_code TEXT', 'failure_code TEXT, retry_not_before INTEGER')
        reset_sql = """CREATE TABLE IF NOT EXISTS relay_thread_resets (
            capture_id INTEGER PRIMARY KEY REFERENCES relay_requests(capture_id),
            thread_id TEXT NOT NULL UNIQUE, reset_message_id TEXT NOT NULL
        )"""
        try:
            with self.transaction():
                exists = self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='relay_threads'").fetchone()
                if exists and 'retry_not_before' not in {
                        row['name'] for row in self.db.execute('PRAGMA table_xinfo(relay_threads)')}:
                    # Accept only the exact old contract before rebuilding its CHECK.
                    # DDL and the copy share one transaction; foreign keys stay enabled.
                    self._validate_route_schema(old_route_sql, legacy=True)
                    self._validate_route_migration_dependencies()
                    self.db.execute(route_sql.replace('IF NOT EXISTS ', '').replace(
                        'relay_threads (', 'relay_threads_cooldown ('))
                    self.db.execute("""INSERT INTO relay_threads_cooldown
                        SELECT capture_id,thread_id,state,attempt_started_at,failure_code,NULL
                        FROM relay_threads""")
                    self.db.execute('DROP TABLE relay_threads')
                    self.db.execute('ALTER TABLE relay_threads_cooldown RENAME TO relay_threads')
                else:
                    self.db.execute(route_sql)
                self._validate_route_schema(route_sql)
                self.db.execute(reset_sql)
                self._validate_reset_schema(reset_sql)
                if self.db.execute('PRAGMA foreign_key_check').fetchone() is not None:
                    raise RuntimeError('incompatible relay thread foreign key schema')
        except BaseException:
            self.db.close()
            raise

    def _validate_route_migration_dependencies(self) -> None:
        # DROP TABLE can run foreign-key cascades even without SQL triggers.
        # No known schema references routes; reject extensions before any rebuild.
        tables = self.db.execute("""SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT GLOB 'sqlite_*'""").fetchall()
        for table in tables:
            name = table['name'].replace('"', '""')
            foreign = self.db.execute(f'PRAGMA foreign_key_list("{name}")').fetchall()
            if any(row['table'].casefold() == 'relay_threads' for row in foreign):
                raise RuntimeError('incompatible relay_threads dependency schema')

    def _validate_route_schema(self, expected_sql: str, *, legacy: bool = False) -> None:
        columns = [tuple(row)[1:] for row in self.db.execute('PRAGMA table_xinfo(relay_threads)')]
        expected = [('capture_id', 'INTEGER', 0, None, 1, 0),
                    ('thread_id', 'TEXT', 0, None, 0, 0),
                    ('state', 'TEXT', 1, None, 0, 0),
                    ('attempt_started_at', 'INTEGER', 0, None, 0, 0),
                    ('failure_code', 'TEXT', 0, None, 0, 0)]
        if not legacy:
            expected.append(('retry_not_before', 'INTEGER', 0, None, 0, 0))
        foreign = [tuple(row) for row in self.db.execute('PRAGMA foreign_key_list(relay_threads)')]
        unique = []
        for index in self.db.execute('PRAGMA index_list(relay_threads)').fetchall():
            if not index['unique'] or index['origin'] != 'u':
                raise RuntimeError('incompatible relay_threads index schema')
            if index['unique']:
                name = index['name'].replace('"', '""')
                unique.append(([row['name'] for row in self.db.execute(f'PRAGMA index_info("{name}")')], index['partial']))
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='relay_threads'").fetchone():
            raise RuntimeError('incompatible relay_threads trigger schema')
        actual_sql = self.db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='relay_threads'").fetchone()[0]
        # Strict declared-shape allowlist also rejects weakened/extra CHECKs,
        # collations, defaults, generated columns and conflict policies.
        def body(sql):
            return ''.join(re.findall(r"'(?:''|[^'])*'|[^\s']+", sql[sql.index('('):])).rstrip(';')
        if (columns != expected or foreign != [(0, 0, 'relay_requests', 'capture_id', 'capture_id', 'NO ACTION', 'NO ACTION', 'NONE')]
                or unique != [(['thread_id'], 0)] or body(actual_sql) != body(expected_sql)):
            raise RuntimeError('incompatible relay_threads schema')

    def _validate_reset_schema(self, expected_sql: str) -> None:
        columns = [tuple(row)[1:] for row in self.db.execute('PRAGMA table_xinfo(relay_thread_resets)')]
        expected = [('capture_id', 'INTEGER', 0, None, 1, 0),
                    ('thread_id', 'TEXT', 1, None, 0, 0),
                    ('reset_message_id', 'TEXT', 1, None, 0, 0)]
        foreign = [tuple(row) for row in self.db.execute('PRAGMA foreign_key_list(relay_thread_resets)')]
        unique = []
        for index in self.db.execute('PRAGMA index_list(relay_thread_resets)').fetchall():
            if index['unique']:
                name = index['name'].replace('"', '""')
                unique.append(([row['name'] for row in self.db.execute(f'PRAGMA index_info("{name}")')], index['partial']))
        actual_sql = self.db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='relay_thread_resets'").fetchone()[0]
        def body(sql):
            return ''.join(re.findall(r"'(?:''|[^'])*'|[^\s']+", sql[sql.index('('):])).rstrip(';')
        if (columns != expected or foreign != [(0, 0, 'relay_requests', 'capture_id', 'capture_id', 'NO ACTION', 'NO ACTION', 'NONE')]
                or unique != [(['thread_id'], 0)] or body(actual_sql) != body(expected_sql)):
            raise RuntimeError('incompatible relay_thread_resets schema')

    def get_route(self, capture_id: int) -> Route | None:
        row = self.db.execute('SELECT * FROM relay_threads WHERE capture_id=?', (capture_id,)).fetchone()
        return Route(**dict(row)) if row else None

    def _thread_authorized(self, req: Request, *, guild_id: str, channel_id: str,
                           approver_id: str, now_ms: int) -> bool:
        if (req.state != 'approved_waiting' or req.message_id is None
                or (req.guild_id, req.channel_id, req.approver_id) != (guild_id, channel_id, approver_id)):
            return False
        if now_ms >= req.expires_at:
            self.db.execute("UPDATE relay_requests SET state='expired' WHERE capture_id=?", (req.capture_id,))
            return False
        original = self.db.execute('SELECT transcript FROM captures WHERE id=?', (req.capture_id,)).fetchone()
        if (original is None or original['transcript'] != req.transcript
                or hashlib.sha256(req.transcript.encode()).hexdigest() != req.digest):
            self.db.execute("UPDATE relay_requests SET state='uncertain' WHERE capture_id=?", (req.capture_id,))
            return False
        return True

    def thread_global_retry_not_before(self, *, guild_id: str, channel_id: str,
                                       approver_id: str, now_ms: int) -> int | None:
        # A Discord global denial outlives its owning capture's approval/reset.
        # Reuse the saved diagnostic and due time so siblings also honor it after restart.
        row = self.db.execute("""SELECT MAX(t.retry_not_before) AS due FROM relay_threads t
            JOIN relay_requests r ON r.capture_id=t.capture_id
            WHERE r.guild_id=? AND r.channel_id=? AND r.approver_id=?
            AND t.failure_code IN ('global_cooldown','http_429_global')
            AND typeof(t.retry_not_before)='integer' AND t.retry_not_before>?""",
            (guild_id, channel_id, approver_id, now_ms)).fetchone()
        return row['due']

    def claim_thread_creation(self, *, guild_id: str, channel_id: str,
                              approver_id: str, now_ms: int) -> Request | None:
        # One bounded examination pass; None can leave eligible work for later ticks.
        scope = dict(guild_id=guild_id, channel_id=channel_id, approver_id=approver_id, now_ms=now_ms)
        with self.transaction():
            if self.thread_global_retry_not_before(**scope) is not None:
                return None
            rows = self.db.execute("""SELECT r.* FROM relay_requests r
                JOIN relay_threads t ON t.capture_id=r.capture_id
                WHERE r.state='approved_waiting' AND r.message_id IS NOT NULL
                AND ((t.state='unbound' AND t.attempt_started_at IS NULL
                    AND t.thread_id IS NULL AND t.retry_not_before IS NULL AND t.failure_code IS NULL)
                  OR (t.state='rate_wait' AND typeof(t.attempt_started_at)='integer'
                    AND t.attempt_started_at>=0 AND typeof(t.retry_not_before)='integer'
                    AND t.retry_not_before>t.attempt_started_at AND t.retry_not_before<=?
                    AND t.failure_code IN ('route_cooldown','global_cooldown',
                        'http_429_route','http_429_shared','http_429_global',
                        'preflight_cancelled')
                    AND (t.thread_id IS NULL OR (t.thread_id=r.message_id
                        AND length(t.thread_id) BETWEEN 17 AND 20
                        AND t.thread_id NOT GLOB '*[^0-9]*'))))
                AND r.guild_id=? AND r.channel_id=? AND r.approver_id=?
                ORDER BY r.capture_id LIMIT 100""", (now_ms, guild_id, channel_id, approver_id))
            for row in rows:
                req = Request(**dict(row))
                if self._thread_authorized(req, **scope):
                    self.db.execute("""UPDATE relay_threads SET state='creating',
                        attempt_started_at=COALESCE(attempt_started_at,?),retry_not_before=NULL
                        WHERE capture_id=? AND state IN ('unbound','rate_wait')""",
                        (now_ms, req.capture_id))
                    return req
            return None

    def authorize_thread_creation(self, capture_id: int, *, expected_request: Request,
                                  guild_id: str, channel_id: str, approver_id: str,
                                  now_ms: int) -> bool:
        """Synchronous authority check at the final native POST admission boundary."""
        with self.transaction():
            req, route = self.get(capture_id), self.get_route(capture_id)
            if (req is None or expected_request != req or req.capture_id != capture_id
                    or route is None or route.state != 'creating'
                    or route.attempt_started_at is None or route.thread_id is not None
                    or route.retry_not_before is not None):
                return False
            scope = dict(guild_id=guild_id, channel_id=channel_id, approver_id=approver_id, now_ms=now_ms)
            return (self._thread_authorized(req, **scope)
                    and self.thread_global_retry_not_before(**scope) is None)

    def defer_thread(self, capture_id: int, *, expected_request: Request,
                     guild_id: str, channel_id: str, approver_id: str,
                     now_ms: int, retry_not_before: int, failure_code: str) -> bool:
        """Persist a safe refusal; return whether its original approval is still valid.

        A denial arriving after reset/expiry still records that no operation happened
        and preserves a global interval for siblings. It never restores authority.
        """
        if (type(now_ms) is not int or now_ms < 0 or type(retry_not_before) is not int
                or not now_ms < retry_not_before <= 2**63 - 1
                or not isinstance(failure_code, str) or failure_code not in COOLDOWN_CODES):
            raise ValueError('invalid thread cooldown')
        with self.transaction():
            req, route = self.get(capture_id), self.get_route(capture_id)
            if (req is None or not isinstance(expected_request, Request)
                    or expected_request.state != 'approved_waiting'
                    or req.state not in ('approved_waiting', 'expired', 'reset', 'rejected')
                    or req != replace(expected_request, state=req.state)
                    or req.capture_id != capture_id
                    or (req.guild_id, req.channel_id, req.approver_id) != (guild_id, channel_id, approver_id)
                    or route is None or route.state not in ('creating', 'ready')
                    or route.attempt_started_at is None
                    or (route.thread_id is not None and (route.thread_id != req.message_id
                        or not re.fullmatch(r'[0-9]{17,20}', route.thread_id)))
                    or (route.state == 'ready' and route.thread_id is None)):
                return False
            authorized = self._thread_authorized(req, guild_id=guild_id, channel_id=channel_id,
                                                  approver_id=approver_id, now_ms=now_ms)
            if not authorized:
                # _thread_authorized does not inspect transcripts for terminal requests.
                # Retain denial metadata only for the exact original capture, never a
                # changed request or an existing failed/uncertain outcome.
                original = self.db.execute('SELECT transcript FROM captures WHERE id=?', (capture_id,)).fetchone()
                if (self.get(capture_id).state not in ('expired', 'reset', 'rejected')
                        or original is None or original['transcript'] != req.transcript
                        or hashlib.sha256(req.transcript.encode()).hexdigest() != req.digest):
                    return False
            self.db.execute("""UPDATE relay_threads SET state=?,retry_not_before=?,failure_code=?
                WHERE capture_id=?""", ('rate_wait' if route.state == 'creating' else 'ready',
                                       retry_not_before, failure_code, capture_id))
            return authorized

    def defer_cancelled_preflight(self, capture_id: int, *, expected_request: Request,
                                  guild_id: str, channel_id: str, approver_id: str,
                                  now_ms: int, retry_not_before: int) -> bool:
        """Resume an interrupted card GET only; caller guarantees no create POST began."""
        if (type(now_ms) is not int or now_ms < 0 or type(retry_not_before) is not int
                or not now_ms < retry_not_before <= 2**63 - 1):
            raise ValueError('invalid preflight retry time')
        with self.transaction():
            req, route = self.get(capture_id), self.get_route(capture_id)
            if (req is None or req != expected_request or req.state != 'approved_waiting'
                    or route is None or route.state != 'creating'
                    or route.thread_id is not None or route.attempt_started_at is None
                    or retry_not_before <= route.attempt_started_at
                    or (req.guild_id, req.channel_id, req.approver_id)
                    != (guild_id, channel_id, approver_id)
                    or not self._thread_authorized(req, guild_id=guild_id,
                        channel_id=channel_id, approver_id=approver_id, now_ms=now_ms)):
                return False
            return self.db.execute("""UPDATE relay_threads SET state='rate_wait',
                retry_not_before=?,failure_code='preflight_cancelled'
                WHERE capture_id=? AND state='creating' AND thread_id IS NULL""",
                (retry_not_before, capture_id)).rowcount == 1

    def remember_thread(self, capture_id: int, thread_id: str) -> None:
        if not isinstance(thread_id, str) or not re.fullmatch(r'[0-9]{17,20}', thread_id):
            raise ValueError('invalid thread identity')
        with self.transaction():
            req, route = self.get(capture_id), self.get_route(capture_id)
            if (req is None or route is None or route.attempt_started_at is None
                    or route.state == 'unbound' or thread_id != req.message_id
                    or route.thread_id not in (None, thread_id)):
                raise RuntimeError('thread identity not claimable')
            if route.thread_id is None:
                self.db.execute('UPDATE relay_threads SET thread_id=? WHERE capture_id=? AND thread_id IS NULL',
                                (thread_id, capture_id))

    def ready_thread(self, capture_id: int, *, guild_id: str, channel_id: str,
                     approver_id: str, now_ms: int,
                     expected_request: Request | None = None) -> bool:
        with self.transaction():
            req, route = self.get(capture_id), self.get_route(capture_id)
            if expected_request is not None and (
                    expected_request.capture_id != capture_id or req != expected_request):
                return False
            if (req is None or route is None or route.state != 'creating'
                    or route.attempt_started_at is None or route.thread_id is None
                    or not re.fullmatch(r'[0-9]{17,20}', route.thread_id)
                    or route.thread_id != req.message_id):
                return False
            if not self._thread_authorized(req, guild_id=guild_id, channel_id=channel_id,
                                           approver_id=approver_id, now_ms=now_ms):
                return False
            return self.db.execute("UPDATE relay_threads SET state='ready',retry_not_before=NULL,failure_code=NULL WHERE capture_id=? AND state='creating'",
                                   (capture_id,)).rowcount == 1

    def fail_thread(self, capture_id: int, *, failure_code: str, uncertain: bool) -> None:
        codes = {'rejected', 'rate_limited', 'unavailable', 'mismatch', 'timeout',
                 'cancelled', 'transport', 'storage', 'interrupted', 'expired', 'reset'}
        if not isinstance(failure_code, str) or failure_code not in codes or type(uncertain) is not bool:
            raise ValueError('invalid thread failure')
        with self.transaction():
            req, route = self.get(capture_id), self.get_route(capture_id)
            # Never let a stale callback undo native execution or a terminal route.
            if (req is None or route is None or route.state not in ('creating', 'ready')
                    or req.state not in ('approved_waiting', 'expired', 'reset', 'uncertain')):
                return
            state = 'uncertain' if uncertain else 'failed'
            self.db.execute('UPDATE relay_threads SET state=?,failure_code=? WHERE capture_id=?',
                            (state, failure_code, capture_id))
            self.db.execute("UPDATE relay_requests SET state=? WHERE capture_id=? AND state='approved_waiting'",
                            (state, capture_id))

    def thread_candidates(self, *, guild_id: str, channel_id: str, approver_id: str,
                          now_ms: int, after: int = 0) -> list[Request]:
        # Enumeration is read-only; integrity and authority are checked at claim.
        if self.thread_global_retry_not_before(guild_id=guild_id, channel_id=channel_id,
                approver_id=approver_id, now_ms=now_ms) is not None:
            return []
        rows = self.db.execute("""SELECT r.* FROM relay_requests r
            JOIN relay_threads t ON t.capture_id=r.capture_id
            WHERE r.guild_id=? AND r.channel_id=? AND r.approver_id=?
            AND r.state='approved_waiting' AND r.expires_at>? AND r.capture_id>?
            AND t.state='ready' AND t.attempt_started_at IS NOT NULL
            AND (t.retry_not_before IS NULL OR (typeof(t.retry_not_before)='integer' AND t.retry_not_before<=?))
            AND t.thread_id=r.message_id AND length(t.thread_id) BETWEEN 17 AND 20
            AND t.thread_id NOT GLOB '*[^0-9]*'
            ORDER BY r.capture_id LIMIT 100""",
            (guild_id, channel_id, approver_id, now_ms, after, now_ms)).fetchall()
        return [Request(**dict(row)) for row in rows]

    def claim_thread_dispatch(self, capture_id: int, *, guild_id: str, channel_id: str,
                              approver_id: str, now_ms: int) -> Request | None:
        with self.transaction():
            req, route = self.get(capture_id), self.get_route(capture_id)
            if (req is None or route is None or route.state != 'ready'
                    or (route.retry_not_before is not None and
                        (type(route.retry_not_before) is not int or route.retry_not_before > now_ms))
                    or route.attempt_started_at is None or route.thread_id is None
                    or not re.fullmatch(r'[0-9]{17,20}', route.thread_id)
                    or route.thread_id != req.message_id):
                return None
            if not self._thread_authorized(req, guild_id=guild_id, channel_id=channel_id,
                                           approver_id=approver_id, now_ms=now_ms):
                return None
            if self.thread_global_retry_not_before(guild_id=guild_id, channel_id=channel_id,
                    approver_id=approver_id, now_ms=now_ms) is not None:
                return None
            self.db.execute("UPDATE relay_requests SET state='dispatching' WHERE capture_id=?", (capture_id,))
            return self.get(capture_id)

    def card_request(self, message_id: str, *, guild_id: str, channel_id: str,
                     approver_id: str) -> Request | None:
        """Find capture-card identity without assuming approval or a ready route."""
        if (not isinstance(message_id, str)
                or re.fullmatch(r'[1-9][0-9]{16,19}', message_id) is None
                or int(message_id) > 2**64 - 1):
            raise ValueError('invalid card identity')
        rows = self.db.execute("""SELECT * FROM relay_requests
            WHERE guild_id=? AND channel_id=? AND approver_id=? AND message_id=?
            LIMIT 2""", (guild_id, channel_id, approver_id, message_id)).fetchall()
        if len(rows) > 1:
            raise RuntimeError('ambiguous capture card identity')
        return Request(**dict(rows[0])) if rows else None

    def thread_request(self, thread_id: str, *, guild_id: str, channel_id: str,
                       approver_id: str) -> Request | None:
        if not isinstance(thread_id, str) or not re.fullmatch(r'[0-9]{17,20}', thread_id):
            raise ValueError('invalid thread identity')
        row = self.db.execute("""SELECT r.* FROM relay_requests r
            JOIN relay_threads t ON t.capture_id=r.capture_id
            WHERE r.guild_id=? AND r.channel_id=? AND r.approver_id=?
            AND r.message_id=? AND t.attempt_started_at IS NOT NULL
            AND t.state!='unbound' AND (t.thread_id=? OR
                (t.thread_id IS NULL AND (t.state IN ('creating','rate_wait','uncertain')
                    OR (t.state='failed' AND r.state IN ('failed','uncertain','expired','reset')))))
            LIMIT 1""", (guild_id, channel_id, approver_id, thread_id, thread_id)).fetchone()
        return Request(**dict(row)) if row else None

    def reset_thread(self, thread_id: str, *, guild_id: str, channel_id: str,
                     approver_id: str, reset_message_id: str | None = None) -> None:
        # Authenticated intent only: this does not certify native reset completion.
        if reset_message_id is not None and (
                not isinstance(reset_message_id, str)
                or not re.fullmatch(r'[1-9][0-9]{16,19}', reset_message_id)
                or int(reset_message_id) > 2**64 - 1):
            raise ValueError('invalid reset message identity')
        with self.transaction():
            req = self.thread_request(thread_id, guild_id=guild_id, channel_id=channel_id,
                                      approver_id=approver_id)
            if req is None:
                if reset_message_id is not None:
                    raise PermissionError('thread reset mapping unavailable')
                return
            self.db.execute('''UPDATE relay_audio_grants SET used=1
                WHERE guild_id=? AND capture_channel_id=? AND actor_id=? AND chat_id=?''',
                (guild_id, channel_id, approver_id, thread_id))
            self.db.execute("""UPDATE relay_requests SET state='reset'
                WHERE capture_id=? AND state IN ('posting','pending','approved_waiting')""",
                (req.capture_id,))

            previous = self.thread_reset_message(thread_id, guild_id=guild_id,
                channel_id=channel_id, approver_id=approver_id) if reset_message_id is not None else None
            if reset_message_id is not None and (previous is None or int(reset_message_id) > int(previous)):
                self.db.execute('''INSERT INTO relay_thread_resets VALUES(?,?,?)
                    ON CONFLICT(capture_id) DO UPDATE SET
                    thread_id=excluded.thread_id,reset_message_id=excluded.reset_message_id''',
                    (req.capture_id, thread_id, reset_message_id))

    def thread_reset_message(self, thread_id: str, *, guild_id: str, channel_id: str,
                             approver_id: str) -> str | None:
        if not isinstance(thread_id, str) or not re.fullmatch(r'[0-9]{17,20}', thread_id):
            raise ValueError('invalid thread identity')
        # Same mapping predicate as thread_request, in one SQLite read snapshot.
        row = self.db.execute('''SELECT reset_message_id FROM relay_thread_resets m
            JOIN relay_requests r ON r.capture_id=m.capture_id
            JOIN relay_threads t ON t.capture_id=r.capture_id
            WHERE m.thread_id=? AND r.guild_id=? AND r.channel_id=? AND r.approver_id=?
            AND r.message_id=? AND t.attempt_started_at IS NOT NULL
            AND t.state!='unbound' AND (t.thread_id=? OR
                (t.thread_id IS NULL AND (t.state IN ('creating','rate_wait','uncertain')
                    OR (t.state='failed' AND r.state IN ('failed','uncertain','expired','reset')))))''',
            (thread_id, guild_id, channel_id, approver_id, thread_id, thread_id)).fetchone()
        return row['reset_message_id'] if row else None

    def grant_audio(self, capture_id: int, *, actor_id: str, guild_id: str,
                    capture_channel_id: str, chat_id: str, message_id: str,
                    profile: str, kind: str, now_ms: int) -> None:
        # Only the deterministic authenticated-event handler may issue grants.
        # This method is NOT a model tool and caller-supplied identities are never exposed.
        with self.transaction():
            record = self.audio_record(capture_id=capture_id, actor_id=actor_id,
                guild_id=guild_id, channel_id=capture_channel_id, now_ms=now_ms)
            self.db.execute("""INSERT OR IGNORE INTO relay_audio_grants
                VALUES(?,?,?,?,?,?,?,?,?,0)""",
                (capture_id, actor_id, guild_id, chat_id, message_id, profile, kind,
                 capture_channel_id, min(now_ms + 300000, record['audio_expires_at'])))

    def claim_audio(self, capture_id: int, *, actor_id: str, guild_id: str,
                    chat_id: str, message_id: str, profile: str, kind: str,
                    now_ms: int) -> dict:
        with self.transaction():
            args = (capture_id, actor_id, guild_id, chat_id, message_id, profile, kind)
            row = self.db.execute("""SELECT * FROM relay_audio_grants WHERE
                capture_id=? AND actor_id=? AND guild_id=? AND chat_id=? AND message_id=?
                AND profile=? AND kind=? AND used=0 AND expires_at>?""", (*args,now_ms)).fetchone()
            if row is None:
                raise PermissionError('explicit audio request missing, expired, or already used')
            record = self.audio_record(capture_id=capture_id, actor_id=actor_id,
                guild_id=guild_id, channel_id=row['capture_channel_id'], now_ms=now_ms)
            self.db.execute("""UPDATE relay_audio_grants SET used=1 WHERE
                capture_id=? AND actor_id=? AND guild_id=? AND chat_id=? AND message_id=?
                AND profile=? AND kind=?""", args)
            return record

    def __enter__(self) -> RelayStore:
        return self

    def __exit__(self, *_args):
        self.db.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            if self.db.in_transaction:
                self.db.execute('ROLLBACK')
            raise

    def cards(self, *, guild_id: str, channel_id: str, approver_id: str,
              after: int = 0) -> list[Request]:
        rows = self.db.execute("""SELECT * FROM relay_requests WHERE guild_id=?
            AND channel_id=? AND approver_id=? AND message_id IS NOT NULL
            AND capture_id>? ORDER BY capture_id LIMIT 100""",
            (guild_id, channel_id, approver_id, after)).fetchall()
        return [Request(**dict(row)) for row in rows]

    def get(self, capture_id: int) -> Request | None:
        row = self.db.execute('SELECT * FROM relay_requests WHERE capture_id=?', (capture_id,)).fetchone()
        return Request(**dict(row)) if row else None

    def claim_card(self, *, guild_id: str, channel_id: str, approver_id: str,
                   now_ms: int, thread_mode: bool = False) -> Request | None:
        if type(thread_mode) is not bool:
            raise ValueError('invalid thread mode')
        if any(not isinstance(value, str) or not re.fullmatch(r'[0-9]{17,20}', value)
               for value in (guild_id, channel_id, approver_id)):
            raise ValueError('invalid capture scope')
        with self.transaction():
            row = self.db.execute('''SELECT * FROM captures c WHERE c.state='pending'
                AND c.discord_uploaded=0 AND c.thread_id IS NULL AND c.hermes_session_id IS NULL
                AND c.answer_text IS NULL AND c.capture_post_token IS NULL AND c.answer_post_token IS NULL
                AND NOT EXISTS(SELECT 1 FROM capture_approvals a WHERE a.capture_id=c.id)
                AND NOT EXISTS(SELECT 1 FROM relay_requests r WHERE r.capture_id=c.id)
                ORDER BY c.id LIMIT 1''').fetchone()
            if row is None:
                return None
            transcript = row['transcript']
            self.db.execute('''INSERT INTO relay_requests(capture_id,guild_id,channel_id,approver_id,
                transcript,digest,nonce,expires_at,audio_expires_at,state)
                VALUES(?,?,?,?,?,?,?,?,?,?)''',
                (row['id'], guild_id, channel_id, approver_id, transcript,
                 hashlib.sha256(transcript.encode()).hexdigest(), secrets.token_hex(16),
                 now_ms + APPROVAL_MS, row['received_at'] + self.audio_retention_ms, 'posting'))
            if thread_mode:
                self.db.execute("INSERT INTO relay_threads(capture_id,state) VALUES(?,'unbound')", (row['id'],))
            return self.get(row['id'])

    def decide(self, *, capture_id: int, actor_id: str, guild_id: str,
               channel_id: str, message_id: str, nonce: str, digest: str,
               now_ms: int, action: str) -> str:
        with self.transaction():
            req = self.get(capture_id)
            if (req is None or action not in {'approve', 'reject'}
                    or (actor_id, guild_id, channel_id, message_id) !=
                       (req.approver_id, req.guild_id, req.channel_id, req.message_id)
                    or not secrets.compare_digest(nonce, req.nonce)
                    or not secrets.compare_digest(digest, req.digest)):
                return 'invalid'
            if req.state != 'pending':
                return 'already_decided'
            original = self.db.execute('SELECT transcript FROM captures WHERE id=?', (capture_id,)).fetchone()
            if (original is None or original['transcript'] != req.transcript
                    or hashlib.sha256(req.transcript.encode()).hexdigest() != req.digest):
                self.db.execute("UPDATE relay_requests SET state='uncertain' WHERE capture_id=?", (capture_id,))
                return 'uncertain'
            state = ('expired' if now_ms >= req.expires_at else
                     'approved_waiting' if action == 'approve' else 'rejected')
            self.db.execute('UPDATE relay_requests SET state=? WHERE capture_id=?', (state, capture_id))
            return state

    def claim_dispatch(self, *, guild_id: str, channel_id: str, approver_id: str,
                       now_ms: int) -> Request | None:
        with self.transaction():
            self.db.execute("""UPDATE relay_requests SET state='expired'
                WHERE state IN ('pending','approved_waiting') AND expires_at<=?
                AND guild_id=? AND channel_id=? AND approver_id=?""",
                (now_ms, guild_id, channel_id, approver_id))
            row = self.db.execute("""SELECT capture_id FROM relay_requests
                WHERE state='approved_waiting' AND guild_id=? AND channel_id=? AND approver_id=?
                AND NOT EXISTS(SELECT 1 FROM relay_threads t WHERE t.capture_id=relay_requests.capture_id)
                ORDER BY capture_id LIMIT 1""", (guild_id, channel_id, approver_id)).fetchone()
            if row is None:
                return None
            req = self.get(row[0])
            original = self.db.execute('SELECT transcript FROM captures WHERE id=?', (row[0],)).fetchone()
            if (original is None or original['transcript'] != req.transcript
                    or hashlib.sha256(req.transcript.encode()).hexdigest() != req.digest):
                self.db.execute("UPDATE relay_requests SET state='uncertain' WHERE capture_id=?", (row[0],))
                return None
            self.db.execute("UPDATE relay_requests SET state='dispatching' WHERE capture_id=?", (row[0],))
            return self.get(row[0])

    def expire_due(self, *, guild_id: str, channel_id: str, approver_id: str,
                   now_ms: int) -> None:
        with self.transaction():
            self.db.execute("""UPDATE relay_requests SET state='expired'
                WHERE state IN ('pending','approved_waiting') AND expires_at<=?
                AND guild_id=? AND channel_id=? AND approver_id=?""",
                (now_ms, guild_id, channel_id, approver_id))

    def expired_audio(self, *, guild_id: str, channel_id: str, approver_id: str,
                      now_ms: int) -> list[dict]:
        rows = self.db.execute("""SELECT c.id,c.audio_path,c.fingerprint FROM captures c
            JOIN relay_requests r ON r.capture_id=c.id WHERE c.audio_path IS NOT NULL
            AND r.audio_expires_at<=? AND r.guild_id=? AND r.channel_id=? AND r.approver_id=?
            ORDER BY c.id LIMIT 100""", (now_ms,guild_id,channel_id,approver_id)).fetchall()
        return [dict(row) for row in rows]

    def forget_audio(self, capture_id: int, path: str) -> None:
        with self.transaction():
            self.db.execute('UPDATE relay_audio_grants SET used=1 WHERE capture_id=?', (capture_id,))
            self.db.execute('UPDATE captures SET audio_path=NULL WHERE id=? AND audio_path=?',
                            (capture_id,path))

    def audio_record(self, *, capture_id: int, actor_id: str, guild_id: str,
                     channel_id: str, now_ms: int) -> dict:
        req = self.get(capture_id)
        if (req is None or req.message_id is None or req.state == 'rejected'
                or (actor_id, guild_id, channel_id) != (req.approver_id, req.guild_id, req.channel_id)
                or now_ms >= req.audio_expires_at):
            raise PermissionError('audio expired or unavailable for this request')
        row = self.db.execute('SELECT audio_path,audio_size,fingerprint FROM captures WHERE id=?',
                              (capture_id,)).fetchone()
        if row is None or row['audio_path'] is None:
            raise PermissionError('audio expired or unavailable for this request')
        return dict(row) | {'audio_expires_at': req.audio_expires_at}

    def recover(self, *, guild_id: str, channel_id: str, approver_id: str, now_ms: int) -> None:
        with self.transaction():
            self.db.execute("""UPDATE relay_requests SET state=CASE
                WHEN state IN ('posting','dispatching') THEN 'uncertain'
                WHEN expires_at<=? AND state IN ('pending','approved_waiting') THEN 'expired'
                ELSE state END WHERE guild_id=? AND channel_id=? AND approver_id=?""",
                (now_ms, guild_id, channel_id, approver_id))
            # Expiry above wins for unstarted approvals; terminal outcomes stay put.
            # Interrupted attempts never regain authority even if no ID was saved.
            self.db.execute("""UPDATE relay_requests SET state='uncertain'
                WHERE state='approved_waiting' AND guild_id=? AND channel_id=? AND approver_id=?
                AND capture_id IN (SELECT capture_id FROM relay_threads WHERE state='creating')""",
                (guild_id, channel_id, approver_id))
            self.db.execute("""UPDATE relay_threads SET state='uncertain',failure_code='interrupted'
                WHERE state='creating' AND capture_id IN (SELECT capture_id FROM relay_requests
                    WHERE guild_id=? AND channel_id=? AND approver_id=?)""",
                (guild_id, channel_id, approver_id))

    def reset(self, *, guild_id: str, channel_id: str, approver_id: str) -> None:
        with self.transaction():
            self.db.execute('''UPDATE relay_audio_grants SET used=1
                WHERE guild_id=? AND capture_channel_id=? AND actor_id=?''',
                (guild_id, channel_id, approver_id))
            self.db.execute("""UPDATE relay_requests SET state='reset'
                WHERE state IN ('posting','pending','approved_waiting')
                AND guild_id=? AND channel_id=? AND approver_id=?""",
                (guild_id, channel_id, approver_id))

    def uncertain(self, capture_id: int) -> None:
        with self.transaction():
            self.db.execute("""UPDATE relay_requests SET state='uncertain'
                WHERE capture_id=? AND state IN ('posting','dispatching')""", (capture_id,))

    def finish(self, capture_id: int) -> None:
        with self.transaction():
            changed = self.db.execute("""UPDATE relay_requests SET state='turn_finished'
                WHERE capture_id=? AND state='dispatching'""", (capture_id,)).rowcount
            if changed != 1:
                raise RuntimeError('execution not in progress')

    def remember_card(self, capture_id: int, message_id: str) -> None:
        # Persist a known remote ID before verification; this grants no authority.
        with self.transaction():
            changed = self.db.execute('''UPDATE relay_requests SET message_id=?
                WHERE capture_id=? AND message_id IS NULL AND state IN ('posting','reset')''',
                (message_id, capture_id)).rowcount
            if changed != 1:
                raise RuntimeError('card identity not claimable')

    def bind_card(self, capture_id: int, message_id: str) -> Request:
        with self.transaction():
            changed = self.db.execute('''UPDATE relay_requests SET message_id=?,state='pending'
                WHERE capture_id=? AND state='posting' ''', (message_id, capture_id)).rowcount
            if changed != 1:
                raise RuntimeError('card operation not claimable')
            return self.get(capture_id)
