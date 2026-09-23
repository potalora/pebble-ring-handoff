"""Local observational worker evidence. Never grants authority or restarts tasks."""
import hashlib
import json
import os
from pathlib import Path
import secrets
import stat

NAME = 'ring-worker-health.json'
FRESH_MS = 30000
CLEANUP_MS = 120000
CREATION_STOP_DIAGNOSTIC = 'Ring thread creation stopped after global HTTP 429; manual recovery required'


def scope_digest(scope):
    return hashlib.sha256(json.dumps(scope, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def incarnation(pid):
    # comm may contain spaces/parentheses; fields after its final ')' start at state.
    fields = Path(f'/proc/{pid}/stat').read_text().rpartition(') ')[2].split()
    if fields[0] in {'Z', 'X'}:
        raise ValueError('process unavailable')
    return int(fields[19])


def directory(data_dir):
    fd = os.open(data_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    metadata = os.fstat(fd)
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        os.close(fd)
        raise ValueError('health unavailable')
    return fd


def write_health(data_dir, scope, *, now_ms, workers_ok, native_ready, cleanup_at_ms,
                 creation_stopped=False):
    if type(creation_stopped) is not bool:
        raise ValueError('invalid health evidence')
    payload = dict(version=2, scope=scope_digest(scope), pid=os.getpid(),
                   start=incarnation(os.getpid()), stamp=now_ms, cleanup=cleanup_at_ms,
                   workers=workers_ok, ready=native_ready, creation_stopped=creation_stopped)
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
    parent = directory(data_dir)
    temporary = '.ring-health-' + secrets.token_hex(12)
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=parent)
        with os.fdopen(fd, 'wb') as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, NAME, src_dir_fd=parent, dst_dir_fd=parent)
    finally:
        try:
            os.unlink(temporary, dir_fd=parent)
        except FileNotFoundError:
            pass
        os.close(parent)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('invalid health evidence')
        result[key] = value
    return result


def read_health(data_dir, scope, now_ms):
    unknown = dict(status='unknown', workers_ok=False, cleanup_ok=False, native_ready=False,
                   creation_stopped=None)
    parent = fd = None
    try:
        parent = directory(data_dir)
        fd = os.open(NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        metadata = os.fstat(fd)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_nlink != 1):
            return unknown
        raw = os.read(fd, 4097)
        if len(raw) > 4096:
            return unknown
        fields = ('st_dev', 'st_ino', 'st_uid', 'st_mode', 'st_nlink', 'st_size',
                  'st_mtime_ns', 'st_ctime_ns')
        for after in (os.fstat(fd), os.stat(NAME, dir_fd=parent, follow_symlinks=False)):
            if any(getattr(metadata, key) != getattr(after, key) for key in fields):
                return unknown
        data = json.loads(raw, object_pairs_hook=unique_object)
        if (type(data) is not dict or set(data) !=
                {'version', 'scope', 'pid', 'start', 'stamp', 'cleanup', 'workers', 'ready', 'creation_stopped'}
                or any(type(data[key]) is not int or data[key] < 0
                       for key in ('version', 'pid', 'start', 'stamp', 'cleanup'))
                or data['version'] != 2 or data['pid'] == 0
                or any(type(data[key]) is not bool for key in ('workers', 'ready', 'creation_stopped'))
                or data['scope'] != scope_digest(scope)
                or type(now_ms) is not int or now_ms < 0):
            return unknown
        try:
            live = incarnation(data['pid']) == data['start']
        except FileNotFoundError:
            live = False
        fresh = 0 <= now_ms-data['stamp'] <= FRESH_MS
        workers = live and fresh and data['workers']
        cleanup = live and fresh and data['cleanup'] > 0 and 0 <= now_ms-data['cleanup'] <= CLEANUP_MS
        ready = live and fresh and data['ready']
        stopped = data['creation_stopped'] if live and fresh else None
        return dict(status='ok' if workers and cleanup and ready and stopped is False else 'degraded',
                    workers_ok=workers, cleanup_ok=cleanup, native_ready=ready,
                    creation_stopped=stopped,
                    **({'diagnostic': CREATION_STOP_DIAGNOSTIC} if stopped is True else {}))
    except (OSError, ValueError, KeyError, TypeError, IndexError):
        return unknown
    finally:
        if fd is not None:
            os.close(fd)
        if parent is not None:
            os.close(parent)
