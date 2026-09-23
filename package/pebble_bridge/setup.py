"""Prepare one protected receiver runtime without changing live captures."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import subprocess
import sys
import contextlib
import io
from typing import Callable

from .management import external_state_path
from .repository import Repository


CODE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNTIME_ROOT = Path(os.environ.get('HERMES_HOME') or Path.home() / '.hermes') / 'pebble-ring-webhook'
CLOUDFLARED_CANDIDATES = tuple(map(Path, (
    '/opt/data/bin/cloudflared', '/usr/local/bin/cloudflared', '/usr/bin/cloudflared')))
HOST_LINE = re.compile(
    r'PEBBLE_ALLOWED_HOSTS=([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.trycloudflare\.com)\n')
TOKEN = re.compile(r'[A-Za-z0-9_-]{32,128}')
SNOWFLAKE = re.compile(r'[0-9]{17,20}')


class SetupError(Exception):
    """A stable setup code, without private path or process details."""


def select_cloudflared(candidates=CLOUDFLARED_CANDIDATES) -> Path:
    for path in candidates:
        if path.is_file() and os.access(path, os.X_OK):
            return path
    raise SetupError('cloudflared-unavailable')


def validate_scope(guild_id: str, channel_id: str, approver_id: str) -> dict[str, str]:
    scope = dict(guild_id=guild_id, channel_id=channel_id, approver_id=approver_id)
    if any(type(value) is not str or SNOWFLAKE.fullmatch(value) is None
           or value[0] == '0' or int(value) > 2**64 - 1
           for value in scope.values()):
        raise SetupError('invalid-scope')
    return scope


def _directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    except OSError as exc:
        raise SetupError('runtime-preparation-failed') from exc
    try:
        metadata = path.lstat()
        if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o700):
            raise SetupError('unsafe-state-path')
    except OSError as exc:
        raise SetupError('unsafe-state-path') from exc


def _private_file(path: Path, content: bytes | None = None) -> bytes:
    """Read a protected file, or create it once without replacing an existing one."""
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    if content is not None and not path.exists() and not path.is_symlink():
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                 os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            pass
        except OSError as exc:
            raise SetupError('runtime-preparation-failed') from exc
        else:
            try:
                remaining = memoryview(content)
                while remaining:
                    written = os.write(descriptor, remaining)
                    if written <= 0:
                        raise SetupError('runtime-preparation-failed')
                    remaining = remaining[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    try:
        descriptor = os.open(path, flags)
        try:
            before = os.fstat(descriptor)
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                    or stat.S_IMODE(before.st_mode) != 0o600
                    or before.st_nlink != 1 or before.st_size > 16384):
                raise SetupError('unsafe-state-path')
            value = os.read(descriptor, 16385)
            after = os.fstat(descriptor)
            if (len(value) != before.st_size or
                    any(getattr(before, key) != getattr(after, key)
                        for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns'))):
                raise SetupError('unsafe-state-path')
            return value
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise SetupError('unsafe-state-path') from exc


def _private_database(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            metadata = os.fstat(descriptor)
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
                    or stat.S_IMODE(metadata.st_mode) != 0o600
                    or metadata.st_nlink != 1):
                raise SetupError('unsafe-state-path')
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise SetupError('unsafe-state-path') from exc


def _receiver_config(path: Path, data_dir: Path) -> bool:
    if not path.exists() and not path.is_symlink():
        return False
    try:
        value = json.loads(_private_file(path))
        if (type(value) is not dict or set(value) != {'data_dir', 'webhook_token', 'port'}
                or value['data_dir'] != str(data_dir) or value['port'] != 8765
                or type(value['webhook_token']) is not str
                or TOKEN.fullmatch(value['webhook_token']) is None):
            raise SetupError('existing-state-mismatch')
    except (UnicodeError, ValueError, TypeError) as exc:
        raise SetupError('existing-state-mismatch') from exc
    return True


def _runtime_host(path: Path) -> bool:
    if not path.exists() and not path.is_symlink():
        return False
    try:
        if HOST_LINE.fullmatch(_private_file(path).decode('ascii')) is None:
            raise SetupError('existing-state-mismatch')
    except UnicodeError as exc:
        raise SetupError('existing-state-mismatch') from exc
    return True


def _install_receiver(venv: Path, requirements: Path) -> None:
    try:
        if not venv.exists():
            created = subprocess.run([sys.executable, '-m', 'venv', str(venv)],
                                     capture_output=True, timeout=120, check=False)
            if created.returncode != 0:
                raise SetupError('receiver-dependencies-failed')
        python = venv / 'bin' / 'python'
        if not python.is_file():
            raise SetupError('receiver-dependencies-failed')
        installed = subprocess.run([str(python), '-m', 'pip', 'install',
                                    '--disable-pip-version-check', '--require-hashes',
                                    '-r', str(requirements)],
                                   capture_output=True, timeout=180, check=False)
        if installed.returncode != 0:
            raise SetupError('receiver-dependencies-failed')
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SetupError('receiver-dependencies-failed') from exc


def _database(data_dir: Path) -> None:
    path = data_dir / 'bridge.sqlite3'
    if path.exists() or path.is_symlink():
        _private_database(path)
        try:
            with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as connection:
                if connection.execute('PRAGMA user_version').fetchone()[0] != 2:
                    raise SetupError('existing-state-mismatch')
        except sqlite3.Error as exc:
            raise SetupError('existing-state-mismatch') from exc
        return
    try:
        repo = Repository(data_dir)
        repo.connection.close()
        _private_database(path)
    except (OSError, sqlite3.Error) as exc:
        raise SetupError('runtime-preparation-failed') from exc


def prepare_runtime(
    runtime_root: Path,
    *,
    cloudflared: Path | None = None,
    install_receiver: Callable[[Path, Path], None] = _install_receiver,
) -> dict[str, str]:
    """Create or verify local state. Existing token and database are never reset."""
    root = Path(runtime_root)
    if (not root.is_absolute() or '..' in root.parts or root.resolve() != root
            or not external_state_path(root)
            or not root.parent.is_dir()):
        raise SetupError('unsafe-state-path')
    cloudflared = cloudflared or select_cloudflared()
    if not cloudflared.is_file() or not os.access(cloudflared, os.X_OK):
        raise SetupError('cloudflared-unavailable')
    if CODE_ROOT.stat().st_uid != os.getuid():
        raise SetupError('plugin-owner-mismatch')
    old_umask = os.umask(0o077)
    try:
        _directory(root)
        data = root / 'data'
        runtime = root / '.pebble-watchdog-runtime'
        _directory(data)
        _directory(runtime)
        secret = root / '.pebble-receiver.json'
        host = root / '.pebble-runtime.env'
        existing_secret = _receiver_config(secret, data)
        existing_host = _runtime_host(host)
        lock = runtime / 'watchdog.lock'
        if lock.exists() or lock.is_symlink():
            _private_file(lock)
        venv = root / '.venv'
        if venv.exists() or venv.is_symlink():
            _directory(venv)
        install_receiver(venv, CODE_ROOT / 'receiver-requirements.txt')
        _directory(venv)
        _database(data)
        if not existing_secret:
            payload = dict(data_dir=str(data), webhook_token=secrets.token_urlsafe(48),
                           port=8765)
            _private_file(secret, (json.dumps(payload, sort_keys=True) + '\n').encode())
            _receiver_config(secret, data)
        if not existing_host:
            _private_file(host, b'PEBBLE_ALLOWED_HOSTS=bootstrap.trycloudflare.com\n')
            _runtime_host(host)
        _private_file(lock, b'')
        return dict(status='runtime-prepared', runtime_root=str(root),
                    data_dir=str(data), token_file=str(secret))
    finally:
        os.umask(old_umask)


def configure_hermes(settings: dict) -> str:
    """Use Hermes's one-write config API; managed installs keep their owner."""
    try:
        from hermes_cli.config import is_managed, set_config_value
    except ImportError:
        return 'unavailable-manual'
    if is_managed():
        return 'managed-manual'
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            set_config_value('plugins.entries.pebble-ring-handoff.settings',
                             json.dumps(settings, sort_keys=True), force=False)
    except (Exception, SystemExit) as exc:
        raise SetupError('hermes-config-failed') from exc
    return 'written'


WATCHDOG_JOB = 'Pebble Ring ingress watchdog'
WATCHDOG_SCRIPT = 'pebble-ring-handoff-watchdog.py'


def _cron_jobs(home: Path) -> list[dict]:
    path = home / 'cron' / 'jobs.json'
    if not path.exists() and not path.is_symlink():
        return []
    try:
        cron = path.parent.lstat()
        metadata = path.lstat()
        if (not stat.S_ISDIR(cron.st_mode) or cron.st_uid != os.getuid()
                or cron.st_mode & 0o022 or not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid() or metadata.st_mode & 0o022
                or metadata.st_size > 2_000_000):
            raise SetupError('watchdog-state-unavailable')
        jobs = json.loads(path.read_text())['jobs']
        if type(jobs) is not list or any(type(item) is not dict for item in jobs):
            raise SetupError('watchdog-state-unavailable')
        return jobs
    except (OSError, UnicodeError, ValueError, TypeError, KeyError) as exc:
        raise SetupError('watchdog-state-unavailable') from exc


def _create_cron_job(command: list[str]) -> None:
    try:
        result = subprocess.run(command, capture_output=True, timeout=30, check=False)
        if result.returncode != 0:
            raise SetupError('watchdog-schedule-failed')
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SetupError('watchdog-schedule-failed') from exc


def _watchdog_job_matches(job: dict, channel_id: str) -> bool:
    return (job.get('script') == WATCHDOG_SCRIPT and job.get('enabled') is True
            and job.get('no_agent') is True
            and job.get('schedule') == {
                'kind': 'interval', 'minutes': 5, 'display': 'every 5m'}
            and job.get('deliver') == f'discord:{channel_id}'
            and job.get('failure_deliver') == f'discord:{channel_id}')


def schedule_watchdog(
    home: Path,
    *,
    channel_id: str,
    create_job: Callable[[list[str]], None] = _create_cron_job,
) -> str:
    """Keep the scheduler wrapper outside the replaceable plugin directory."""
    home = Path(home)
    if type(channel_id) is not str or SNOWFLAKE.fullmatch(channel_id) is None:
        raise SetupError('invalid-scope')
    if not home.is_absolute() or '..' in home.parts or home.resolve() != home:
        raise SetupError('watchdog-state-unavailable')
    try:
        metadata = home.stat()
        if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid()
                or metadata.st_mode & 0o022):
            raise SetupError('watchdog-state-unavailable')
        scripts = home / 'scripts'
        scripts.mkdir(mode=0o700, exist_ok=True)
        metadata = scripts.lstat()
        if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid()
                or metadata.st_mode & 0o022):
            raise SetupError('watchdog-state-unavailable')
        wrapper = scripts / WATCHDOG_SCRIPT
        source = (CODE_ROOT / 'scripts' / 'pebble_watchdog_job.py').read_bytes()
        if _private_file(wrapper, source) != source:
            raise SetupError('watchdog-script-mismatch')
        matches = [job for job in _cron_jobs(home) if job.get('name') == WATCHDOG_JOB]
        if matches:
            if len(matches) != 1 or not _watchdog_job_matches(matches[0], channel_id):
                raise SetupError('watchdog-job-conflict')
            return 'existing'
        cli = Path(sys.executable).with_name('hermes')
        delivery = f'discord:{channel_id}'
        create_job([str(cli), 'cron', 'create', '5m', '--name', WATCHDOG_JOB,
                    '--script', WATCHDOG_SCRIPT, '--no-agent', '--deliver', delivery,
                    '--failure-deliver', delivery])
        matches = [job for job in _cron_jobs(home) if job.get('name') == WATCHDOG_JOB]
        if len(matches) != 1 or not _watchdog_job_matches(matches[0], channel_id):
            raise SetupError('watchdog-schedule-unverified')
        return 'created'
    except OSError as exc:
        raise SetupError('watchdog-state-unavailable') from exc


def guided_setup(
    *,
    guild_id: str,
    channel_id: str,
    approver_id: str,
    runtime_root: Path | None = None,
    existing_data_dir: Path | None = None,
    existing_scope: dict | None = None,
) -> dict:
    """Prepare local state, apply one config mapping, and verify ingress once."""
    scope = validate_scope(guild_id, channel_id, approver_id)
    root = Path(runtime_root or (
        existing_data_dir.parent if existing_data_dir is not None else DEFAULT_RUNTIME_ROOT))
    if existing_data_dir is not None and (
            existing_data_dir != root / 'data' or existing_scope != scope):
        raise SetupError('configured-scope-mismatch')
    local = prepare_runtime(root)
    settings = dict(data_dir=local['data_dir'], **scope,
                    audio_retention_ms=604_800_000)
    config = ('already-configured' if existing_data_dir is not None
              else configure_hermes(settings))
    from .management import recover, status
    ingress = recover(Path(local['data_dir']), scope)
    if ingress.get('status') != 'ingress-verified':
        return dict(status='ingress-unverified', config=config, watchdog='not-scheduled',
                    token_file=local['token_file'], ingress_url=None,
                    gateway_restart_required=True)
    watchdog = 'manual'
    watchdog_error = None
    if config in {'written', 'already-configured'}:
        home = Path(os.environ.get('HERMES_HOME') or Path.home() / '.hermes')
        try:
            watchdog = schedule_watchdog(home, channel_id=scope['channel_id'])
        except SetupError as exc:
            watchdog_error = str(exc)
    ingress_url = ingress.get('ingress_url') or status(Path(local['data_dir']), scope).get('ingress_url')
    return dict(
        status=('setup-prepared' if watchdog in {'created', 'existing'}
                and config in {'written', 'already-configured'} else 'setup-needs-operator'),
        config=config, watchdog=watchdog, token_file=local['token_file'],
        watchdog_error=watchdog_error, ingress_url=ingress_url,
        gateway_restart_required=True)
