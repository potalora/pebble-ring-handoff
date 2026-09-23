"""Read-only state summary, not a process/network health check. No default paths."""
import json
import importlib.util
from pathlib import Path
import os
import re
import sqlite3
import stat
import sys
import tempfile
import time
from contextlib import contextmanager


# Four 15-second operation budgets allow creation/readback and bookkeeping grace.
# This is an observation threshold, never a retry or execution deadline.
CREATING_STALE_MS = 60_000


def route_counts(db, scope, now_ms):
    values = db.execute('''SELECT
        COALESCE(SUM(t.state='failed'),0),
        COALESCE(SUM(t.state='uncertain'),0),
        COALESCE(SUM(t.state='creating' AND t.attempt_started_at<?),0)
        FROM relay_threads t JOIN relay_requests r ON r.capture_id=t.capture_id
        WHERE r.guild_id=? AND r.channel_id=? AND r.approver_id=?''',
        (now_ms-CREATING_STALE_MS, *scope)).fetchone()
    return dict(zip(('failed_routes', 'uncertain_routes', 'stale_creating_routes'), values))


@contextmanager
def snapshot(data_dir):
    # Even mode=ro may create WAL/SHM files. Query a private, stable file copy
    # instead; never use immutable=1 on the source (that silently ignores WAL).
    fields = ('st_dev', 'st_ino', 'st_mode', 'st_uid', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
    opened = []
    missing = []
    try:
        for name in ('bridge.sqlite3', 'bridge.sqlite3-wal'):
            path = data_dir/name
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            except FileNotFoundError:
                if name.endswith('-wal'):
                    missing.append(path)
                    continue
                raise
            opened.append((path, fd, os.fstat(fd)))
        with tempfile.TemporaryDirectory(prefix='ring-health-') as temporary:
            target = Path(temporary)
            for path, fd, before in opened:
                if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                        or before.st_size > 256*1024*1024):
                    raise ValueError('state file unavailable')
                with (target/path.name).open('xb') as output:
                    remaining = before.st_size
                    while remaining:
                        chunk = os.read(fd, min(remaining, 1024*1024))
                        if not chunk:
                            raise ValueError('state changed')
                        output.write(chunk)
                        remaining -= len(chunk)
            for path, fd, before in opened:
                for after in (os.fstat(fd), path.stat(follow_symlinks=False)):
                    if any(getattr(before, key) != getattr(after, key) for key in fields):
                        raise ValueError('state changed during snapshot')
            if any(path.exists() for path in missing):
                raise ValueError('state changed during snapshot')
            yield target
    finally:
        for _, fd, _ in opened:
            os.close(fd)


def report(data_dir, guild_id, channel_id, approver_id, *, now_ms=None):
    if not data_dir.is_absolute() or any(
            re.fullmatch(r'[0-9]{17,20}', value) is None
            for value in (guild_id, channel_id, approver_id)):
        raise ValueError('explicit scope required')
    with snapshot(data_dir) as frozen:
        result = query_snapshot(frozen, (guild_id, channel_id, approver_id), now_ms=now_ms)
    package_root = Path(__file__).resolve().parents[1]
    source = package_root / 'pebble_bridge/worker_health.py'
    if not source.is_file():
        source = package_root / 'src/pebble_bridge/worker_health.py'
    spec = importlib.util.spec_from_file_location('successor_worker_health', source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    workers = module.read_health(data_dir, dict(guild_id=guild_id, channel_id=channel_id,
        approver_id=approver_id), time.time_ns() // 1_000_000 if now_ms is None else now_ms)
    result['creation_stopped'] = workers['creation_stopped']
    if workers['creation_stopped'] is True:
        result.update(status='manual_review_required', diagnostic=module.CREATION_STOP_DIAGNOSTIC)
    elif workers['creation_stopped'] is None and result['status'] == 'state_readable':
        result['status'] = 'unknown'
    return result


def query_snapshot(data_dir, scope, *, now_ms=None):
    now_ms = time.time_ns() // 1_000_000 if now_ms is None else now_ms
    db = sqlite3.connect((data_dir/'bridge.sqlite3').as_uri()+'?mode=ro', uri=True, timeout=0.075)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        if db.execute('PRAGMA user_version').fetchone()[0] != 2:
            raise ValueError('unsupported state')
        counts = dict(db.execute('''SELECT state,COUNT(*) FROM relay_requests
            WHERE guild_id=? AND channel_id=? AND approver_id=? GROUP BY state''', scope))
        if not set(counts) <= {'posting', 'pending', 'approved_waiting', 'dispatching',
                'uncertain', 'failed', 'expired', 'rejected', 'reset', 'turn_finished'}:
            raise ValueError('unsupported state')
        unbound = db.execute('''SELECT COUNT(*) FROM relay_requests WHERE
            guild_id=? AND channel_id=? AND approver_id=? AND state='uncertain'
            AND message_id IS NULL''', scope).fetchone()[0]
        # Before claiming, ingress has no Discord scope. Report this explicitly
        # as database-wide backlog, not as scoped execution authority.
        backlog = db.execute('''SELECT COUNT(*), MIN(c.received_at) FROM captures c
            WHERE c.state='pending' AND c.discord_uploaded=0 AND c.thread_id IS NULL
            AND c.hermes_session_id IS NULL AND c.answer_text IS NULL
            AND c.capture_post_token IS NULL AND c.answer_post_token IS NULL
            AND NOT EXISTS(SELECT 1 FROM capture_approvals a WHERE a.capture_id=c.id)
            AND NOT EXISTS(SELECT 1 FROM relay_requests r WHERE r.capture_id=c.id)''').fetchone()
        routes = route_counts(db, scope, now_ms)
        status = 'manual_review_required' if (counts.get('uncertain', 0)
            or counts.get('failed', 0) or any(routes.values())) else 'state_readable'
        return dict(status=status, requests=counts, unbound_uncertain=unbound,
                    **routes,
                    unclaimed_ingress=dict(count=backlog[0], oldest_received_at_ms=backlog[1]))
    finally:
        db.close()


if __name__ == '__main__':
    try:
        if len(sys.argv) != 5:
            raise ValueError('explicit state directory and scope required')
        result = report(Path(sys.argv[1]), *sys.argv[2:])
    except (OSError, ValueError, sqlite3.Error):
        print('{"status":"unavailable"}')
        raise SystemExit(1)
    print(json.dumps(result, sort_keys=True))
    raise SystemExit({'manual_review_required': 2, 'state_readable': 0}.get(result['status'], 1))
