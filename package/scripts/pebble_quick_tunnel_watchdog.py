"""Deterministic Pebble Quick Tunnel watchdog."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import re
import secrets
import select
import signal
import socket
import sqlite3
import ssl
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

FAILURE = "Pebble Quick Tunnel watchdog recovery failed\n"
HOST_PATTERN = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.trycloudflare\.com"
)
GENERATION_PATTERN = re.compile(r"g(?:0|[1-9][0-9]*)")
NONCE_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,128}")
BRIDGE_NONCE = "bridge"
QUICK_TUNNEL_API_HOST = "api.trycloudflare.com"
TOKEN_PATTERN = re.compile(r"[!-~]{16,256}")
URL_PATTERN = re.compile(r"https?://[^\s]+", re.IGNORECASE)
MAX_TRANSCRIPT_LINE_BYTES = 2048
MAX_TRANSCRIPT_LINES = 64
MAX_TRANSCRIPT_BYTES = 64 * 1024
MAX_RUNTIME_BYTES = 1024
MAX_STARTUP_READ_BYTES = 4096
MAX_PROC_BYTES = 8192
STARTUP_SCAN_SECONDS = 15.0
STARTUP_POLL_SECONDS = 0.1
READY_MARGIN_SECONDS = 5.0
READY_POLL_SECONDS = 0.05
READY_TIMEOUT_SECONDS = STARTUP_SCAN_SECONDS + READY_MARGIN_SECONDS
CHILD_STOP_SECONDS = 1.0
STOP_POLL_SECONDS = 0.05
CHILD_IDENTITY_SECONDS = 2.0
CHILD_IDENTITY_POLL_SECONDS = 0.05
LOOPBACK_HEALTH_ATTEMPTS = 20
LOOPBACK_HEALTH_POLL_SECONDS = 0.05
PUBLIC_DNS_SETTLE_SECONDS = 3.0
PUBLIC_HEALTH_ATTEMPTS = 3
PUBLIC_HEALTH_POLL_SECONDS = 1.0
TRANSIENT_PUBLIC_STATUSES = frozenset({502, 503, 504, 530})
SQLITE_BUSY_TIMEOUT_SECONDS = 1.0
# Accounting budget for configured recovery waits, including management's final
# verification. This is not an enforced wall-clock timeout: DNS resolution and
# filesystem operations may take longer. The native cron runner owns its own
# configurable script deadline; the packaged shell wrapper imposes none.
RECOVERY_WAIT_BUDGET_SECONDS = 90.0
MIN_RECOVERY_MARGIN_SECONDS = 5.0
CLOUDFLARED = next(
    (candidate for candidate in (
        "/opt/data/bin/cloudflared", "/usr/local/bin/cloudflared", "/usr/bin/cloudflared")
     if os.path.isfile(candidate) and os.access(candidate, os.X_OK)),
    "/opt/data/bin/cloudflared",
)
LOOPBACK_ORIGIN = "http://127.0.0.1:8765"
CODE_ROOT = Path(__file__).resolve().parents[1]
STATUS_TIMEOUT_SECONDS = 5.0
LOOPBACK_STATUS_TIMEOUT_SECONDS = 0.2
AUTHENTICATED_PROBE_BOUNDARY = "pebble-watchdog-health-probe"
AUTHENTICATED_PROBE_BODY = (
    b"--pebble-watchdog-health-probe\r\n"
    b'Content-Disposition: form-data; name="client"\r\n\r\n'
    b"ring\r\n"
    b"--pebble-watchdog-health-probe\r\n"
    b'Content-Disposition: form-data; name="recordedAt"\r\n\r\n'
    b"0\r\n"
    b"--pebble-watchdog-health-probe\r\n"
    b'Content-Disposition: form-data; name="transcription"\r\n\r\n'
    b"Pebble watchdog health probe\r\n"
    b"--pebble-watchdog-health-probe\r\n"
    b'Content-Disposition: form-data; name="test"\r\n\r\n'
    b"true\r\n"
    b"--pebble-watchdog-health-probe--\r\n"
)


def no_redirect_opener() -> urllib.request.OpenerDirector:
    """Build an opener that never follows redirects and never uses a proxy."""
    opener = urllib.request.OpenerDirector()
    opener.add_handler(urllib.request.HTTPHandler())
    opener.add_handler(urllib.request.HTTPSHandler())
    return opener


_NO_REDIRECT_OPENER = no_redirect_opener()


def request_status(
    url: str,
    headers: dict[str, str],
    *,
    open_url: Callable[..., object] = _NO_REDIRECT_OPENER.open,
) -> int:
    """Return the direct response status without following redirects."""
    if type(url) is not str or not url.startswith(("http://", "https://")):
        raise WatchdogError
    if type(headers) is not dict:
        raise WatchdogError
    request = urllib.request.Request(url, headers=headers, method="GET")
    timeout = (
        LOOPBACK_STATUS_TIMEOUT_SECONDS
        if url == f"{LOOPBACK_ORIGIN}/pebble"
        else STATUS_TIMEOUT_SECONDS
    )
    try:
        with open_url(request, timeout=timeout) as response:
            status = response.getcode()
    except (urllib.error.URLError, OSError) as exc:
        raise WatchdogError from exc
    if type(status) is not int:
        raise WatchdogError
    return status


def authenticated_request_status(
    url: str,
    headers: dict[str, str],
    *,
    open_url: Callable[..., object] = _NO_REDIRECT_OPENER.open,
) -> int:
    """POST the bridge's fixed authenticated no-capture test event."""
    if type(url) is not str or not url.startswith("https://"):
        raise WatchdogError
    if type(headers) is not dict or set(headers) != {"Authorization"}:
        raise WatchdogError
    authorization = headers["Authorization"]
    if (
        type(authorization) is not str
        or not authorization.startswith("Bearer ")
        or TOKEN_PATTERN.fullmatch(authorization[len("Bearer ") :]) is None
    ):
        raise WatchdogError
    request = urllib.request.Request(
        url,
        data=AUTHENTICATED_PROBE_BODY,
        headers={
            "Authorization": authorization,
            "Content-Type": (
                "multipart/form-data; boundary=" + AUTHENTICATED_PROBE_BOUNDARY
            ),
            "X-Index-Test": "true",
            "X-Index-Trigger": "test-event",
        },
        method="POST",
    )
    try:
        with open_url(request, timeout=STATUS_TIMEOUT_SECONDS) as response:
            status = response.getcode()
    except (urllib.error.URLError, OSError) as exc:
        raise WatchdogError from exc
    if type(status) is not int:
        raise WatchdogError
    return status


def read_bearer_token(path: Path) -> str:
    """Use the same protected receiver parser, never the gateway environment."""
    import importlib.util

    _secure_metadata(path.parent, directory=True, mode=0o700)
    try:
        spec = importlib.util.spec_from_file_location(
            '_ring_receiver_launcher', Path(__file__).resolve().with_name('run_receiver.py'))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        config = module.load_config(path, path.with_name('.pebble-runtime.env'))
        if (config['data_dir'] != str(path.parent / 'data')
                or config['port'] != urlsplit(LOOPBACK_ORIGIN).port):
            raise ValueError('supervised receiver scope mismatch')
        return config['webhook_token']
    except (OSError, ValueError, TypeError, KeyError, ImportError) as exc:
        raise WatchdogError from exc


def make_authenticated_probe(
    token: str,
    send: Callable[[str, dict[str, str]], int] | None = None,
) -> Callable[[str], int]:
    """Build an authenticated probe that keeps the bearer out of argv and state."""
    if type(token) is not str or TOKEN_PATTERN.fullmatch(token) is None:
        raise WatchdogError
    sender = authenticated_request_status if send is None else send

    def probe(url: str) -> int:
        return sender(url, {"Authorization": f"Bearer {token}"})

    return probe


class WatchdogError(Exception):
    """A deliberately non-diagnostic watchdog failure."""


@dataclass(frozen=True)
class Command:
    role: str
    nonce: str | None
    state_root: Path


@dataclass(frozen=True)
class ReadyRecord:
    hostname: str
    nonce: str
    status: str


@dataclass(frozen=True)
class ProcessRecord:
    pid: int
    starttime: int
    role: str
    nonce: str


@dataclass(frozen=True)
class ProcessObservation:
    starttime: int
    argv: tuple[str, ...]
    pgid: int


class ProcessClass(Enum):
    OWNED = "owned"
    GONE = "gone"
    CONFLICT = "conflict"


@dataclass(frozen=True)
class Manifest:
    generation: str
    hostname: str
    verified_at: int
    bridge: ProcessRecord
    supervisor: ProcessRecord


@dataclass(frozen=True)
class ControllerSnapshot:
    runtime_hostname: str
    database_present: bool
    manifest: Manifest | None
    readiness: ReadyRecord | None
    bridge_class: ProcessClass | None
    supervisor_class: ProcessClass | None


@dataclass(frozen=True)
class Paths:
    root: Path
    code_root: Path
    runtime_dir: Path
    runtime_env: Path
    secret_env: Path
    lock: Path
    manifest: Path
    readiness: Path
    database: Path
    launcher: Path
    python: Path
    script: Path

    @classmethod
    def for_root(cls, root: Path) -> Paths:
        root = Path(root)
        if not root.is_absolute() or ".." in root.parts:
            raise WatchdogError
        # Native force-install replaces the code tree. Durable state must be
        # disjoint from it, including paths obscured by filesystem aliases.
        try:
            lexical_code = Path(__file__).absolute().parents[1]
            pairs = ((root, lexical_code), (root, CODE_ROOT),
                     (root.resolve(), CODE_ROOT))
            if any(state.is_relative_to(code) or code.is_relative_to(state)
                   for state, code in pairs):
                raise WatchdogError
        except (OSError, RuntimeError) as exc:
            raise WatchdogError from exc
        runtime_dir = root / ".pebble-watchdog-runtime"
        return cls(
            root=root,
            code_root=CODE_ROOT,
            runtime_dir=runtime_dir,
            runtime_env=root / ".pebble-runtime.env",
            secret_env=root / ".pebble-receiver.json",
            lock=runtime_dir / "watchdog.lock",
            manifest=runtime_dir / "manifest.json",
            readiness=runtime_dir / "ready.json",
            database=root / "data" / "bridge.sqlite3",
            launcher=CODE_ROOT / "scripts" / "run_receiver.py",
            python=root / ".venv" / "bin" / "python",
            script=Path(__file__).resolve(),
        )


def _secure_metadata(
    path: Path, *, directory: bool, mode: int, optional: bool = False
) -> os.stat_result | None:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError as exc:
        if optional:
            return None
        raise WatchdogError from exc
    except OSError as exc:
        raise WatchdogError from exc
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    if (
        not expected_type(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise WatchdogError
    return metadata


def _read_validated_file(
    path: Path, expected: os.stat_result, *, max_bytes: int
) -> bytes:
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        try:
            current = os.fstat(descriptor)
            if (
                (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino)
                or not stat.S_ISREG(current.st_mode)
                or current.st_uid != os.getuid()
                or stat.S_IMODE(current.st_mode) != 0o600
            ):
                raise WatchdogError
            chunks: list[bytes] = []
            remaining = max_bytes + 1
            while remaining:
                chunk = os.read(descriptor, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise WatchdogError from exc
    data = b"".join(chunks)
    if len(data) > max_bytes:
        raise WatchdogError
    return data


def _atomic_write_protected(path: Path, data: bytes) -> None:
    parent_metadata = _secure_metadata(path.parent, directory=True, mode=0o700)
    assert parent_metadata is not None
    _secure_metadata(path, directory=False, mode=0o600, optional=True)
    descriptor = -1
    temporary = ""
    try:
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{path.name}.", dir=path.parent
        )
        os.fchmod(descriptor, 0o600)
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise WatchdogError
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        temporary = ""
        directory = os.open(
            path.parent, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
        )
        try:
            current = os.fstat(directory)
            if (current.st_dev, current.st_ino) != (
                parent_metadata.st_dev,
                parent_metadata.st_ino,
            ):
                raise WatchdogError
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError as exc:
        raise WatchdogError from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


def _read_runtime_file(path: Path) -> tuple[bytes, str]:
    metadata = _secure_metadata(path, directory=False, mode=0o600)
    assert metadata is not None
    data = _read_validated_file(path, metadata, max_bytes=MAX_RUNTIME_BYTES)
    try:
        value = data.decode("ascii")
    except UnicodeError as exc:
        raise WatchdogError from exc
    match = re.fullmatch(r"PEBBLE_ALLOWED_HOSTS=(.+)\n", value)
    if match is None or HOST_PATTERN.fullmatch(match.group(1)) is None:
        raise WatchdogError
    return data, match.group(1)


def rewrite_runtime_hostname(path: Path, hostname: str) -> bytes:
    """Atomically replace only the canonical runtime hostname and return prior bytes."""
    if type(hostname) is not str or HOST_PATTERN.fullmatch(hostname) is None:
        raise WatchdogError
    _secure_metadata(path.parent, directory=True, mode=0o700)
    original, _ = _read_runtime_file(path)
    _atomic_write_protected(
        path, f"PEBBLE_ALLOWED_HOSTS={hostname}\n".encode("ascii")
    )
    return original


def restore_runtime(path: Path, original: bytes) -> None:
    """Restore exact previously validated runtime bytes atomically."""
    if type(original) is not bytes:
        raise WatchdogError
    _secure_metadata(path.parent, directory=True, mode=0o700)
    try:
        value = original.decode("ascii")
    except UnicodeError as exc:
        raise WatchdogError from exc
    match = re.fullmatch(r"PEBBLE_ALLOWED_HOSTS=(.+)\n", value)
    if match is None or HOST_PATTERN.fullmatch(match.group(1)) is None:
        raise WatchdogError
    _atomic_write_protected(path, original)


def try_acquire_watchdog_lock(path: Path) -> int | None:
    """Acquire the private watchdog lock without waiting; overlap is not an error."""
    _secure_metadata(path.parent, directory=True, mode=0o700)
    expected = _secure_metadata(path, directory=False, mode=0o600)
    assert expected is not None
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW)
        current = os.fstat(descriptor)
        if (
            (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino)
            or not stat.S_ISREG(current.st_mode)
            or current.st_uid != os.getuid()
            or stat.S_IMODE(current.st_mode) != 0o600
        ):
            raise WatchdogError
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            return None
        return descriptor
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        raise WatchdogError from exc
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        raise


def preflight(paths: Paths) -> str:
    """Read the sole non-secret runtime datum without opening secrets."""
    _secure_metadata(paths.root, directory=True, mode=0o700)
    _secure_metadata(paths.runtime_dir, directory=True, mode=0o700)
    _secure_metadata(paths.database.parent, directory=True, mode=0o700)
    _secure_metadata(
        paths.database, directory=False, mode=0o600, optional=True
    )
    runtime_metadata = _secure_metadata(paths.runtime_env, directory=False, mode=0o600)
    assert runtime_metadata is not None
    for path in (paths.secret_env, paths.lock):
        _secure_metadata(path, directory=False, mode=0o600)
    for path in (paths.manifest, paths.readiness):
        _secure_metadata(path, directory=False, mode=0o600, optional=True)
    try:
        value = _read_validated_file(
            paths.runtime_env, runtime_metadata, max_bytes=MAX_RUNTIME_BYTES
        ).decode("ascii")
    except (OSError, UnicodeError) as exc:
        raise WatchdogError from exc
    match = re.fullmatch(r"PEBBLE_ALLOWED_HOSTS=(.+)\n", value)
    if match is None or HOST_PATTERN.fullmatch(match.group(1)) is None:
        raise WatchdogError
    return match.group(1)


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise WatchdogError
        value[key] = item
    return value


def _validated_process_record(value: object, *, role: str) -> ProcessRecord:
    if not isinstance(value, dict) or set(value) != {"pid", "starttime", "role", "nonce"}:
        raise WatchdogError
    record = ProcessRecord(
        pid=value["pid"],
        starttime=value["starttime"],
        role=value["role"],
        nonce=value["nonce"],
    )
    if (
        type(record.pid) is not int
        or record.pid <= 1
        or type(record.starttime) is not int
        or record.starttime <= 0
        or type(record.role) is not str
        or record.role != role
        or type(record.nonce) is not str
        or NONCE_PATTERN.fullmatch(record.nonce) is None
        or (role == "bridge" and record.nonce != BRIDGE_NONCE)
    ):
        raise WatchdogError
    return record


def load_manifest(path: Path) -> Manifest | None:
    """Load absent bootstrap state or one bounded private typed manifest."""
    _secure_metadata(path.parent, directory=True, mode=0o700)
    metadata = _secure_metadata(path, directory=False, mode=0o600, optional=True)
    if metadata is None:
        return None
    data = _read_validated_file(path, metadata, max_bytes=MAX_PROC_BYTES)
    if not data.endswith(b"\n") or data.count(b"\n") != 1:
        raise WatchdogError
    try:
        value = json.loads(data, object_pairs_hook=_unique_json_object)
        if not isinstance(value, dict) or set(value) != {
            "generation",
            "hostname",
            "verified_at",
            "owned",
        }:
            raise WatchdogError
        generation = value["generation"]
        hostname = value["hostname"]
        verified_at = value["verified_at"]
        owned = value["owned"]
        if not isinstance(owned, dict) or set(owned) != {"bridge", "supervisor"}:
            raise WatchdogError
        bridge = _validated_process_record(owned["bridge"], role="bridge")
        supervisor = _validated_process_record(
            owned["supervisor"], role="supervisor"
        )
    except (KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise WatchdogError from exc
    if (
        type(generation) is not str
        or GENERATION_PATTERN.fullmatch(generation) is None
        or type(hostname) is not str
        or HOST_PATTERN.fullmatch(hostname) is None
        or type(verified_at) is not int
        or verified_at < 0
    ):
        raise WatchdogError
    return Manifest(
        generation=generation,
        hostname=hostname,
        verified_at=verified_at,
        bridge=bridge,
        supervisor=supervisor,
    )


def _process_record_payload(record: ProcessRecord) -> dict[str, object]:
    payload: dict[str, object] = {
        "pid": record.pid,
        "starttime": record.starttime,
        "role": record.role,
        "nonce": record.nonce,
    }
    _validated_process_record(payload, role=record.role)
    return payload


def commit_manifest(
    path: Path,
    *,
    previous: Manifest | None,
    hostname: str,
    verified_at: int,
    bridge: ProcessRecord,
    supervisor: ProcessRecord,
) -> Manifest:
    """Atomically commit one exact next-generation sanitized manifest."""
    if (
        type(hostname) is not str
        or HOST_PATTERN.fullmatch(hostname) is None
        or type(verified_at) is not int
        or verified_at < 0
        or bridge.role != "bridge"
        or supervisor.role != "supervisor"
    ):
        raise WatchdogError
    if previous is None:
        generation_number = 0
    elif (
        isinstance(previous, Manifest)
        and GENERATION_PATTERN.fullmatch(previous.generation) is not None
    ):
        generation_number = int(previous.generation[1:])
    else:
        raise WatchdogError
    manifest = Manifest(
        generation=f"g{generation_number + 1}",
        hostname=hostname,
        verified_at=verified_at,
        bridge=bridge,
        supervisor=supervisor,
    )
    payload = {
        "generation": manifest.generation,
        "hostname": manifest.hostname,
        "owned": {
            "bridge": _process_record_payload(manifest.bridge),
            "supervisor": _process_record_payload(manifest.supervisor),
        },
        "verified_at": manifest.verified_at,
    }
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
    _atomic_write_protected(path, data)
    return manifest


def _read_bounded_nofollow(path: Path, *, max_bytes: int) -> bytes:
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    data = b"".join(chunks)
    if len(data) > max_bytes:
        raise WatchdogError
    return data


def observe_process(
    pid: int,
    *,
    proc_root: Path = Path("/proc"),
    getpgid: Callable[[int], int] = os.getpgid,
) -> ProcessObservation | None:
    """Read one bounded process identity from procfs and the kernel process group."""
    if type(pid) is not int or pid <= 1:
        raise WatchdogError
    process_root = Path(proc_root) / str(pid)
    try:
        stat_data = _read_bounded_nofollow(process_root / "stat", max_bytes=MAX_PROC_BYTES)
        cmdline = _read_bounded_nofollow(
            process_root / "cmdline", max_bytes=MAX_PROC_BYTES
        )
        pgid = getpgid(pid)
    except (FileNotFoundError, ProcessLookupError):
        return None
    except OSError as exc:
        raise WatchdogError from exc

    prefix = f"{pid} (".encode("ascii")
    comm_end = stat_data.rfind(b") ")
    stat_fields = stat_data[comm_end + 2 :].split() if comm_end >= len(prefix) else []
    if (
        not stat_data.startswith(prefix)
        or len(stat_fields) <= 19
        or not stat_fields[19].isdigit()
        or type(pgid) is not int
        or pgid <= 1
    ):
        raise WatchdogError
    starttime = int(stat_fields[19])
    if starttime <= 0:
        raise WatchdogError
    if stat_fields[0] == b"Z":
        return None
    if not cmdline:
        # Exit can clear cmdline after the first live stat read. Confirm that
        # transition once, without treating malformed or reused identity as gone.
        try:
            final_stat = _read_bounded_nofollow(process_root / "stat", max_bytes=MAX_PROC_BYTES)
        except (FileNotFoundError, ProcessLookupError):
            return None
        except OSError as exc:
            raise WatchdogError from exc
        final_end = final_stat.rfind(b") ")
        final_fields = final_stat[final_end + 2:].split() if final_end >= len(prefix) else []
        if (final_stat.startswith(prefix) and len(final_fields) > 19
                and final_fields[0] == b"Z" and final_fields[19].isdigit()
                and int(final_fields[19]) == starttime):
            return None
        raise WatchdogError
    if not cmdline.endswith(b"\0"):
        raise WatchdogError
    argv_bytes = cmdline[:-1].split(b"\0")
    if not argv_bytes or any(not item for item in argv_bytes):
        raise WatchdogError
    try:
        argv = tuple(item.decode("utf-8") for item in argv_bytes)
    except UnicodeError as exc:
        raise WatchdogError from exc
    return ProcessObservation(starttime=starttime, argv=argv, pgid=pgid)


def supervisor_argv(paths: Paths, nonce: str) -> tuple[str, ...]:
    """Return the exact detached supervisor command for a nonce."""
    return (
        str(paths.python),
        str(paths.script),
        "--state-root",
        str(paths.root),
        "--tunnel-supervisor",
        "--ready-nonce",
        nonce,
    )


def bridge_launcher_argv(paths: Paths) -> tuple[str, ...]:
    """Isolated receiver launch: private JSON plus sole hostname trust anchor."""
    return (str(paths.python), "-I", "-B", str(paths.launcher),
            str(paths.secret_env), str(paths.runtime_env))


def bridge_argv(paths: Paths) -> tuple[str, ...]:
    return (str(paths.python), "-I", "-B", str(paths.code_root / "scripts" / "serve_receiver.py"))


def validate_process_record(record: ProcessRecord, *, role: str) -> ProcessRecord:
    """Validate a typed record and return it unchanged."""
    _validated_process_record(_process_record_payload(record), role=role)
    return record


def start_supervisor(
    paths: Paths,
    nonce: str,
    *,
    spawn: Callable[..., object] = subprocess.Popen,
    observe: Callable[[int], ProcessObservation | None] = observe_process,
) -> ProcessRecord:
    """Launch the detached supervisor and record its start identity."""
    if type(nonce) is not str or NONCE_PATTERN.fullmatch(nonce) is None:
        raise WatchdogError
    argv = supervisor_argv(paths, nonce)
    try:
        child = spawn(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
            bufsize=0,
        )
    except OSError as exc:
        raise WatchdogError from exc
    pid = getattr(child, "pid", None)
    if type(pid) is not int or pid <= 1:
        raise WatchdogError
    observation = observe(pid)
    if observation is None:
        raise WatchdogError
    return validate_process_record(
        ProcessRecord(pid=pid, starttime=observation.starttime, role="supervisor", nonce=nonce),
        role="supervisor",
    )


def start_bridge(
    paths: Paths,
    *,
    spawn: Callable[..., object] = subprocess.Popen,
    observe: Callable[[int], ProcessObservation | None] = observe_process,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ProcessRecord:
    """Launch the bridge and wait boundedly for its exact final identity."""
    try:
        child = spawn(
            bridge_launcher_argv(paths),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
            bufsize=0,
            env={
                "HOME": str(paths.root.parent),
                "PATH": "/usr/local/bin:/usr/bin:/bin",
            },
        )
    except OSError as exc:
        raise WatchdogError from exc
    try:
        pid = getattr(child, "pid", None)
        if type(pid) is not int or pid <= 1:
            raise WatchdogError
        expected_argv = bridge_argv(paths)
        deadline = monotonic() + CHILD_IDENTITY_SECONDS
        starttime: int | None = None
        while True:
            observation = observe(pid)
            if observation is None:
                raise WatchdogError
            if starttime is None:
                starttime = observation.starttime
            if observation.starttime != starttime or observation.pgid != pid:
                raise WatchdogError
            if observation.argv == expected_argv:
                return validate_process_record(
                    ProcessRecord(
                        pid=pid,
                        starttime=starttime,
                        role="bridge",
                        nonce=BRIDGE_NONCE,
                    ),
                    role="bridge",
                )
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise WatchdogError
            sleep(min(CHILD_IDENTITY_POLL_SECONDS, remaining))
    except BaseException as failure:
        try:
            terminate_started_child(child)
        except WatchdogError:
            raise WatchdogError from failure
        raise


def new_nonce() -> str:
    """Return a fresh URL-safe nonce of sufficient entropy."""
    return secrets.token_urlsafe(24)


def wait_supervisor(
    paths: Paths,
    nonce: str,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Wait for exact nonce-bound readiness from the detached supervisor."""
    return wait_for_readiness(
        expected_nonce=nonce,
        read_ready=lambda: load_readiness(paths.readiness),
        monotonic=monotonic,
        sleep=sleep,
    )


def stop_process(
    paths: Paths,
    record: ProcessRecord,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Stop a strongly owned detached process group with identity rechecks."""
    stop_owned_process(
        paths=paths,
        record=record,
        observe=observe_process,
        signal_group=os.killpg,
        monotonic=monotonic,
        sleep=sleep,
    )


def controller_actions(paths: Paths) -> dict[str, object]:
    """Build the real injected action set that drives the controller."""

    def request(url: str, headers: dict[str, str]) -> int:
        return request_status(url, headers)

    def probe(url: str) -> int:
        token = read_bearer_token(paths.secret_env)
        return make_authenticated_probe(token)(url)

    def healthy(snapshot: ControllerSnapshot) -> bool:
        return verify_healthy(
            snapshot,
            paths=paths,
            request_status=request,
            authenticated_probe=probe,
        )

    def bridge_only(snapshot: ControllerSnapshot) -> bool:
        return recover_bridge_only(
            snapshot,
            paths=paths,
            start_bridge=lambda: start_bridge(paths),
            stop_bridge=lambda record: stop_process(paths, record),
            request_status=request,
            authenticated_probe=probe,
            verified_at=int(time.time()),
        )

    def full(snapshot: ControllerSnapshot) -> str:
        return recover_full(
            snapshot,
            paths=paths,
            new_nonce=new_nonce,
            stop_process=lambda record: stop_process(paths, record),
            start_supervisor=lambda nonce: start_supervisor(paths, nonce),
            wait_supervisor=lambda nonce: wait_supervisor(paths, nonce),
            start_bridge=lambda: start_bridge(paths),
            request_status=request,
            authenticated_probe=probe,
            verified_at=int(time.time()),
        )

    def prepare(candidate: Paths) -> ControllerSnapshot:
        snapshot = prepare_controller(candidate)
        if (snapshot.bridge_class is ProcessClass.CONFLICT
                or snapshot.supervisor_class is ProcessClass.CONFLICT):
            raise WatchdogError
        if snapshot.database_present:
            # Validate receiver inputs after persisted-state/ownership checks,
            # but before any recovery process or HTTP action. Discard the token.
            read_bearer_token(candidate.secret_env)
        return snapshot

    return {
        "acquire": try_acquire_watchdog_lock,
        "prepare": prepare,
        "healthy": healthy,
        "bridge_only": bridge_only,
        "full": full,
    }


def classify_process(
    *,
    paths: Paths,
    record: ProcessRecord,
    observe: Callable[[int], ProcessObservation | None],
) -> ProcessClass:
    """Classify exact process ownership without conflating conflicts with absence."""
    if (
        type(record.pid) is not int
        or record.pid <= 1
        or type(record.starttime) is not int
        or record.starttime <= 0
        or type(record.role) is not str
        or record.role not in {"supervisor", "bridge"}
        or type(record.nonce) is not str
        or NONCE_PATTERN.fullmatch(record.nonce) is None
    ):
        raise WatchdogError
    observation = observe(record.pid)
    if observation is None:
        return ProcessClass.GONE
    if record.role == "supervisor":
        expected_argv = supervisor_argv(paths, record.nonce)
    elif record.role == "bridge":
        if record.nonce != BRIDGE_NONCE:
            return ProcessClass.CONFLICT
        expected_argv = bridge_argv(paths)
    else:
        return ProcessClass.CONFLICT
    if (
        observation.starttime == record.starttime
        and observation.argv == expected_argv
        and observation.pgid == record.pid
    ):
        return ProcessClass.OWNED
    return ProcessClass.CONFLICT


def stop_owned_process(
    *,
    paths: Paths,
    record: ProcessRecord,
    observe: Callable[[int], ProcessObservation | None],
    signal_group: Callable[[int, int], None],
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> None:
    """Fail closed unless the recorded PID is absent or strongly owned."""
    classification = classify_process(paths=paths, record=record, observe=observe)
    if classification is ProcessClass.GONE:
        return
    if classification is ProcessClass.CONFLICT:
        raise WatchdogError
    try:
        signal_group(record.pid, signal.SIGTERM)
    except OSError as exc:
        raise WatchdogError from exc
    deadline = monotonic() + CHILD_STOP_SECONDS
    while True:
        classification = classify_process(paths=paths, record=record, observe=observe)
        if classification is ProcessClass.GONE:
            return
        if classification is ProcessClass.CONFLICT:
            raise WatchdogError
        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        sleep(min(STOP_POLL_SECONDS, remaining))
    try:
        signal_group(record.pid, signal.SIGKILL)
    except OSError as exc:
        raise WatchdogError from exc
    deadline = monotonic() + CHILD_STOP_SECONDS
    while True:
        classification = classify_process(paths=paths, record=record, observe=observe)
        if classification is ProcessClass.GONE:
            return
        if classification is ProcessClass.CONFLICT:
            raise WatchdogError
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise WatchdogError
        sleep(min(STOP_POLL_SECONDS, remaining))


def parse_quick_tunnel_origin(transcript: bytes) -> str:
    """Select one canonical Quick Tunnel origin from a complete transcript."""
    lines = transcript.splitlines()
    if (
        len(transcript) > MAX_TRANSCRIPT_BYTES
        or not transcript.endswith(b"\n")
        or len(lines) > MAX_TRANSCRIPT_LINES
        or any(len(line) > MAX_TRANSCRIPT_LINE_BYTES for line in lines)
    ):
        raise WatchdogError
    try:
        text = transcript.decode("utf-8")
    except UnicodeError as exc:
        raise WatchdogError from exc
    origins: list[str] = []
    for token in URL_PATTERN.findall(text):
        try:
            parts = urlsplit(token)
            host = parts.hostname or ""
        except ValueError as exc:
            if HOST_PATTERN.search(unquote(token).lower()) is not None:
                raise WatchdogError from exc
            continue
        if any(
            value is not None and HOST_PATTERN.search(unquote(value)) is not None
            for value in (parts.username, parts.password)
        ):
            raise WatchdogError
        lowered = host.lower()
        if lowered == QUICK_TUNNEL_API_HOST:
            continue
        if HOST_PATTERN.fullmatch(lowered):
            if token != f"https://{lowered}":
                raise WatchdogError
            origins.append(lowered)
        elif lowered.endswith(".trycloudflare.com"):
            raise WatchdogError
    if len(origins) != 1:
        raise WatchdogError
    return origins[0]


def read_pipe_once(descriptor: int, timeout: float) -> bytes | None:
    """Read one bounded startup chunk, preserving timeout versus EOF."""
    try:
        readable, _, _ = select.select([descriptor], [], [], timeout)
        if not readable:
            return None
        return os.read(descriptor, MAX_STARTUP_READ_BYTES)
    except OSError as exc:
        raise WatchdogError from exc


def tunnel_argv() -> tuple[str, ...]:
    """Return the sole permitted direct Cloudflared invocation."""
    return (CLOUDFLARED, "tunnel", "--no-autoupdate", "--url", LOOPBACK_ORIGIN)


def launch_tunnel_child(
    *, spawn: Callable[..., object] = subprocess.Popen
) -> object:
    """Launch Cloudflared directly with one unbuffered combined output pipe."""
    return spawn(
        tunnel_argv(),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
        close_fds=True,
    )


def terminate_started_child(child: object) -> None:
    """Terminate and boundedly reap a child started by this supervisor."""
    terminate = getattr(child, "terminate", None)
    wait = getattr(child, "wait", None)
    if not callable(terminate) or not callable(wait):
        raise WatchdogError
    try:
        terminate()
        try:
            wait(timeout=CHILD_STOP_SECONDS)
        except subprocess.TimeoutExpired:
            kill = getattr(child, "kill", None)
            if not callable(kill):
                raise WatchdogError
            kill()
            wait(timeout=CHILD_STOP_SECONDS)
    except (OSError, subprocess.SubprocessError) as exc:
        raise WatchdogError from exc


def discard_tunnel_output(
    child: object, *, read: Callable[[int, int], bytes] = os.read
) -> int:
    """Discard bounded output chunks until Cloudflared exits, then reap it."""
    output = getattr(child, "stdout", None)
    wait = getattr(child, "wait", None)
    if output is None or not callable(getattr(output, "fileno", None)) or not callable(wait):
        raise WatchdogError
    try:
        descriptor = output.fileno()
        if type(descriptor) is not int or descriptor < 0:
            raise WatchdogError
        while read(descriptor, MAX_STARTUP_READ_BYTES):
            pass
        returncode = wait()
    except OSError as exc:
        raise WatchdogError from exc
    if type(returncode) is not int:
        raise WatchdogError
    return returncode


def establish_tunnel_readiness(
    *,
    paths: Paths,
    nonce: str,
    start_child: Callable[[], object] = launch_tunnel_child,
    read_pipe: Callable[[int, float], bytes | None] = read_pipe_once,
    monotonic: Callable[[], float] = time.monotonic,
) -> object:
    """Start Cloudflared and publish readiness after its complete startup window."""
    child = start_child()
    output = getattr(child, "stdout", None)
    poll = getattr(child, "poll", None)
    if output is None or not callable(getattr(output, "fileno", None)) or not callable(poll):
        raise WatchdogError
    try:
        descriptor = output.fileno()
    except (OSError, ValueError) as exc:
        raise WatchdogError from exc
    if type(descriptor) is not int or descriptor < 0:
        raise WatchdogError
    try:
        complete_tunnel_startup(
            nonce=nonce,
            read_once=lambda timeout: read_pipe(descriptor, timeout),
            monotonic=monotonic,
            child_alive=lambda: poll() is None,
            publish=lambda record: publish_readiness(paths.readiness, record),
        )
    except BaseException:
        terminate_started_child(child)
        raise
    return child


def capture_startup_origin(
    *,
    read_once: Callable[[float], bytes | None],
    monotonic: Callable[[], float],
    child_alive: Callable[[], bool],
) -> str:
    """Capture the complete fixed startup window before accepting an origin."""
    deadline = monotonic() + STARTUP_SCAN_SECONDS
    chunks: list[bytes] = []
    total = 0
    lines = 0
    partial_line = 0

    def append(chunk: bytes) -> None:
        nonlocal lines, partial_line, total
        total += len(chunk)
        if total > MAX_TRANSCRIPT_BYTES:
            raise WatchdogError
        segments = chunk.split(b"\n")
        if len(segments) == 1:
            partial_line += len(chunk)
        else:
            if partial_line + len(segments[0]) > MAX_TRANSCRIPT_LINE_BYTES:
                raise WatchdogError
            if any(
                len(segment) > MAX_TRANSCRIPT_LINE_BYTES
                for segment in segments[1:-1]
            ):
                raise WatchdogError
            lines += len(segments) - 1
            partial_line = len(segments[-1])
        if lines > MAX_TRANSCRIPT_LINES or partial_line > MAX_TRANSCRIPT_LINE_BYTES:
            raise WatchdogError
        chunks.append(chunk)

    while monotonic() < deadline:
        timeout = min(STARTUP_POLL_SECONDS, deadline - monotonic())
        chunk = read_once(timeout)
        if chunk == b"":
            raise WatchdogError
        if chunk is not None:
            append(chunk)
    while True:
        chunk = read_once(0.0)
        if chunk is None:
            break
        if chunk == b"":
            raise WatchdogError
        append(chunk)
    if not child_alive():
        raise WatchdogError
    return parse_quick_tunnel_origin(b"".join(chunks))


def complete_tunnel_startup(
    *,
    nonce: str,
    read_once: Callable[[float], bytes | None],
    monotonic: Callable[[], float],
    child_alive: Callable[[], bool],
    publish: Callable[[ReadyRecord], None],
) -> str:
    """Publish nonce-bound readiness only after complete transcript acceptance."""
    if NONCE_PATTERN.fullmatch(nonce) is None:
        raise WatchdogError
    hostname = capture_startup_origin(
        read_once=read_once,
        monotonic=monotonic,
        child_alive=child_alive,
    )
    publish(ReadyRecord(hostname=hostname, nonce=nonce, status="ready"))
    return hostname


def publish_readiness(path: Path, record: ReadyRecord) -> None:
    """Atomically publish one strict sanitized readiness record."""
    if (
        record.status != "ready"
        or NONCE_PATTERN.fullmatch(record.nonce) is None
        or HOST_PATTERN.fullmatch(record.hostname) is None
    ):
        raise WatchdogError
    payload = json.dumps(
        {
            "hostname": record.hostname,
            "nonce": record.nonce,
            "status": record.status,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii") + b"\n"
    _atomic_write_protected(path, payload)


def load_readiness(path: Path) -> ReadyRecord | None:
    """Return absent readiness only; reject every malformed present record."""
    _secure_metadata(path.parent, directory=True, mode=0o700)
    metadata = _secure_metadata(path, directory=False, mode=0o600, optional=True)
    if metadata is None:
        return None
    data = _read_validated_file(path, metadata, max_bytes=1024)
    if not data.endswith(b"\n") or data.count(b"\n") != 1:
        raise WatchdogError

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise WatchdogError
            value[key] = item
        return value

    try:
        value = json.loads(data, object_pairs_hook=unique_object)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise WatchdogError from exc
    if not isinstance(value, dict) or set(value) != {"hostname", "nonce", "status"}:
        raise WatchdogError
    hostname = value["hostname"]
    nonce = value["nonce"]
    status_value = value["status"]
    if (
        type(hostname) is not str
        or type(nonce) is not str
        or type(status_value) is not str
        or HOST_PATTERN.fullmatch(hostname) is None
        or NONCE_PATTERN.fullmatch(nonce) is None
        or status_value != "ready"
    ):
        raise WatchdogError
    return ReadyRecord(hostname=hostname, nonce=nonce, status=status_value)


def remove_readiness(path: Path) -> None:
    """Remove transient readiness only when it is a safe validated record."""
    _secure_metadata(path.parent, directory=True, mode=0o700)
    metadata = _secure_metadata(path, directory=False, mode=0o600, optional=True)
    if metadata is None:
        return
    _read_validated_file(path, metadata, max_bytes=1024)
    try:
        os.unlink(path)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise WatchdogError from exc
    _secure_metadata(path.parent, directory=True, mode=0o700)


def wait_for_readiness(
    *,
    expected_nonce: str,
    read_ready: Callable[[], ReadyRecord | None],
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> str:
    """Poll only absent readiness through the final derived deadline read."""
    if NONCE_PATTERN.fullmatch(expected_nonce) is None:
        raise WatchdogError
    deadline = monotonic() + READY_TIMEOUT_SECONDS
    while True:
        record = read_ready()
        if record is not None:
            if (
                record.nonce != expected_nonce
                or record.status != "ready"
                or HOST_PATTERN.fullmatch(record.hostname) is None
            ):
                raise WatchdogError
            return record.hostname
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise WatchdogError
        sleep(min(READY_POLL_SECONDS, remaining))


def read_capture_count(path: Path) -> int | None:
    """Read the bridge capture count without creating or mutating its database."""
    _secure_metadata(path.parent, directory=True, mode=0o700)
    metadata = _secure_metadata(path, directory=False, mode=0o600, optional=True)
    if metadata is None:
        return None
    connection: sqlite3.Connection | None = None
    try:
        absolute = os.path.abspath(path)
        uri = f"file:{quote(absolute, safe='/')}?mode=ro"
        connection = sqlite3.connect(
            uri, uri=True, timeout=SQLITE_BUSY_TIMEOUT_SECONDS
        )
        connection.execute("PRAGMA query_only=ON")
        row = connection.execute("SELECT COUNT(*) FROM captures").fetchone()
        if (
            row is None
            or len(row) != 1
            or type(row[0]) is not int
            or row[0] < 0
        ):
            raise WatchdogError
    except sqlite3.Error as exc:
        raise WatchdogError from exc
    finally:
        if connection is not None:
            connection.close()
    current = _secure_metadata(path, directory=False, mode=0o600)
    assert current is not None
    if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
        raise WatchdogError
    return row[0]


def prove_authenticated_no_capture(
    *,
    hostname: str,
    database: Path,
    probe: Callable[[str], int],
) -> None:
    """Require a canonical direct 204 probe with unchanged durable capture count."""
    if type(hostname) is not str or HOST_PATTERN.fullmatch(hostname) is None:
        raise WatchdogError
    target = f"https://{hostname}/pebble"
    before = read_capture_count(database)
    if before is None:
        raise WatchdogError
    status = probe(target)
    if type(status) is not int or status != 204:
        raise WatchdogError
    after = read_capture_count(database)
    if after is None or after != before:
        raise WatchdogError


def transient_public_failure(error: WatchdogError) -> bool:
    """Recognize retryable transport failures without weakening TLS checks."""
    reason = error.__cause__
    if isinstance(reason, urllib.error.URLError):
        reason = reason.reason
    if isinstance(reason, ssl.SSLError):
        return False
    if isinstance(reason, socket.gaierror):
        return reason.errno in {socket.EAI_AGAIN, socket.EAI_NONAME}
    return isinstance(reason, OSError) and (
        isinstance(reason, (ConnectionError, TimeoutError))
        or reason.errno in {
            errno.ECONNREFUSED, errno.ECONNRESET, errno.ECONNABORTED,
            errno.ETIMEDOUT, errno.ENETUNREACH, errno.EHOSTUNREACH,
        }
    )


def prove_health(
    hostname: str,
    *,
    request_status: Callable[[str, dict[str, str]], int],
    sleep: Callable[[float], None] = time.sleep,
    public_settle_seconds: float = 0.0,
) -> None:
    """Require direct 405 from the fixed loopback and canonical public routes."""
    if type(hostname) is not str or HOST_PATTERN.fullmatch(hostname) is None:
        raise WatchdogError
    loopback_url = f"{LOOPBACK_ORIGIN}/pebble"
    for attempt in range(LOOPBACK_HEALTH_ATTEMPTS):
        try:
            loopback_status = request_status(loopback_url, {})
        except WatchdogError:
            loopback_status = None
        if type(loopback_status) is int and loopback_status == 405:
            break
        if attempt + 1 == LOOPBACK_HEALTH_ATTEMPTS:
            raise WatchdogError
        sleep(LOOPBACK_HEALTH_POLL_SECONDS)

    if public_settle_seconds > 0:
        sleep(public_settle_seconds)
    # Only full recovery supplies a settling window for a newly issued host.
    # Existing-host verification and bridge-only recovery remain one-shot.
    attempts = PUBLIC_HEALTH_ATTEMPTS if public_settle_seconds > 0 else 1
    for attempt in range(attempts):
        try:
            public_status = request_status(f"https://{hostname}/pebble", {})
        except WatchdogError as exc:
            if attempt + 1 == attempts or not transient_public_failure(exc):
                raise
        else:
            if type(public_status) is int and public_status == 405:
                return
            if (type(public_status) is not int
                    or public_status not in TRANSIENT_PUBLIC_STATUSES
                    or attempt + 1 == attempts):
                raise WatchdogError
        sleep(PUBLIC_HEALTH_POLL_SECONDS)


def recover_bridge_only(
    snapshot: ControllerSnapshot,
    *,
    paths: Paths,
    start_bridge: Callable[[], ProcessRecord],
    stop_bridge: Callable[[ProcessRecord], None],
    request_status: Callable[[str, dict[str, str]], int],
    authenticated_probe: Callable[[str], int],
    verified_at: int,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Replace only a gone bridge while preserving an owned verified tunnel."""
    if (
        snapshot.bridge_class is ProcessClass.CONFLICT
        or snapshot.supervisor_class is ProcessClass.CONFLICT
    ):
        raise WatchdogError
    manifest = snapshot.manifest
    readiness = snapshot.readiness
    if (
        not snapshot.database_present
        or manifest is None
        or readiness is None
        or snapshot.bridge_class is not ProcessClass.GONE
        or snapshot.supervisor_class is not ProcessClass.OWNED
        or snapshot.runtime_hostname != manifest.hostname
        or readiness.hostname != manifest.hostname
        or readiness.nonce != manifest.supervisor.nonce
        or readiness.status != "ready"
    ):
        return False
    bridge = start_bridge()
    try:
        _validated_process_record(_process_record_payload(bridge), role="bridge")
        prove_health(
            manifest.hostname,
            request_status=request_status,
            sleep=sleep,
        )
        prove_authenticated_no_capture(
            hostname=manifest.hostname,
            database=paths.database,
            probe=authenticated_probe,
        )
        commit_manifest(
            paths.manifest,
            previous=manifest,
            hostname=manifest.hostname,
            verified_at=verified_at,
            bridge=bridge,
            supervisor=manifest.supervisor,
        )
    except BaseException:
        stop_bridge(bridge)
        raise
    return True


def recover_full(
    snapshot: ControllerSnapshot,
    *,
    paths: Paths,
    new_nonce: Callable[[], str],
    stop_process: Callable[[ProcessRecord], None],
    start_supervisor: Callable[[str], ProcessRecord],
    wait_supervisor: Callable[[str], str],
    start_bridge: Callable[[], ProcessRecord],
    request_status: Callable[[str, dict[str, str]], int],
    authenticated_probe: Callable[[str], int],
    verified_at: int,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Run one transaction-shaped full ingress replacement and return its host."""
    if (
        snapshot.bridge_class is ProcessClass.CONFLICT
        or snapshot.supervisor_class is ProcessClass.CONFLICT
    ):
        raise WatchdogError
    if not snapshot.database_present:
        raise WatchdogError
    manifest = snapshot.manifest
    stopped: list[ProcessRecord] = []
    supervisor_record: ProcessRecord | None = None
    bridge_record: ProcessRecord | None = None
    runtime_original: bytes | None = None
    nonce = new_nonce()
    if type(nonce) is not str or NONCE_PATTERN.fullmatch(nonce) is None:
        raise WatchdogError
    remove_readiness(paths.readiness)
    try:
        if manifest is not None:
            for record, classification in (
                (manifest.bridge, snapshot.bridge_class),
                (manifest.supervisor, snapshot.supervisor_class),
            ):
                if classification is ProcessClass.OWNED:
                    stop_process(record)
                    stopped.append(record)
        supervisor_record = start_supervisor(nonce)
        _validated_process_record(
            _process_record_payload(supervisor_record), role="supervisor"
        )
        hostname = wait_supervisor(nonce)
        if type(hostname) is not str or HOST_PATTERN.fullmatch(hostname) is None:
            raise WatchdogError
        runtime_original = rewrite_runtime_hostname(paths.runtime_env, hostname)
        bridge_record = start_bridge()
        _validated_process_record(
            _process_record_payload(bridge_record), role="bridge"
        )
        prove_health(
            hostname,
            request_status=request_status,
            sleep=sleep,
            public_settle_seconds=PUBLIC_DNS_SETTLE_SECONDS,
        )
        prove_authenticated_no_capture(
            hostname=hostname,
            database=paths.database,
            probe=authenticated_probe,
        )
        commit_manifest(
            paths.manifest,
            previous=manifest,
            hostname=hostname,
            verified_at=verified_at,
            bridge=bridge_record,
            supervisor=supervisor_record,
        )
        return hostname
    except BaseException as failure:
        cleanup_failed = False
        cleanup_actions: tuple[Callable[[], None], ...] = tuple(
            action
            for action in (
                (lambda: stop_process(bridge_record))
                if bridge_record is not None
                else None,
                (lambda: stop_process(supervisor_record))
                if supervisor_record is not None
                else None,
                (lambda: restore_runtime(paths.runtime_env, runtime_original))
                if runtime_original is not None
                else None,
                lambda: remove_readiness(paths.readiness),
            )
            if action is not None
        )
        for action in cleanup_actions:
            try:
                action()
            except WatchdogError:
                cleanup_failed = True
        if cleanup_failed:
            raise WatchdogError from failure
        raise


def verify_healthy(
    snapshot: ControllerSnapshot,
    *,
    paths: Paths,
    request_status: Callable[[str, dict[str, str]], int],
    authenticated_probe: Callable[[str], int],
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Verify a fully matching owned generation without mutating it."""
    if (
        snapshot.bridge_class is ProcessClass.CONFLICT
        or snapshot.supervisor_class is ProcessClass.CONFLICT
    ):
        raise WatchdogError
    manifest = snapshot.manifest
    readiness = snapshot.readiness
    if (
        not snapshot.database_present
        or manifest is None
        or readiness is None
        or snapshot.bridge_class is not ProcessClass.OWNED
        or snapshot.supervisor_class is not ProcessClass.OWNED
        or snapshot.runtime_hostname != manifest.hostname
        or readiness.hostname != manifest.hostname
        or readiness.nonce != manifest.supervisor.nonce
        or readiness.status != "ready"
    ):
        return False
    prove_health(manifest.hostname, request_status=request_status, sleep=sleep)
    prove_authenticated_no_capture(
        hostname=manifest.hostname,
        database=paths.database,
        probe=authenticated_probe,
    )
    return True


def prepare_controller(
    paths: Paths,
    *,
    observe: Callable[[int], ProcessObservation | None] = observe_process,
) -> ControllerSnapshot:
    """Validate all persisted state before any process classification."""
    runtime_hostname = preflight(paths)
    manifest = load_manifest(paths.manifest)
    readiness = load_readiness(paths.readiness)
    database_present = (
        _secure_metadata(
            paths.database, directory=False, mode=0o600, optional=True
        )
        is not None
    )
    if manifest is None or not database_present:
        return ControllerSnapshot(
            runtime_hostname=runtime_hostname,
            database_present=database_present,
            manifest=manifest,
            readiness=readiness,
            bridge_class=None,
            supervisor_class=None,
        )
    bridge_class = classify_process(
        paths=paths, record=manifest.bridge, observe=observe
    )
    supervisor_class = classify_process(
        paths=paths, record=manifest.supervisor, observe=observe
    )
    return ControllerSnapshot(
        runtime_hostname=runtime_hostname,
        database_present=database_present,
        manifest=manifest,
        readiness=readiness,
        bridge_class=bridge_class,
        supervisor_class=supervisor_class,
    )


def supervise_tunnel(*, paths: Paths, nonce: str) -> int:
    """Establish sanitized readiness, then discard output until child exit."""
    child = establish_tunnel_readiness(paths=paths, nonce=nonce)
    return discard_tunnel_output(child)


def parse_cli(argv: Sequence[str]) -> Command:
    """Accept an explicit state root and a nonce-bound execution role."""
    if len(argv) < 2 or argv[0] != "--state-root":
        raise WatchdogError
    state_root = Path(argv[1])
    if not state_root.is_absolute() or ".." in state_root.parts:
        raise WatchdogError
    role_args = list(argv[2:])
    if not role_args:
        return Command("watchdog", None, state_root)
    if (
        len(role_args) == 3
        and role_args[:2] == ["--tunnel-supervisor", "--ready-nonce"]
        and NONCE_PATTERN.fullmatch(role_args[2]) is not None
    ):
        return Command("supervisor", role_args[2], state_root)
    raise WatchdogError


def format_recovery_output(hostname: str) -> str:
    """Return the sole permitted recovery success line for a canonical host."""
    if type(hostname) is not str or HOST_PATTERN.fullmatch(hostname) is None:
        raise WatchdogError
    return f"Pebble ingress URL: https://{hostname}/pebble\n"


def render_watchdog_result(result: str | None) -> tuple[int, str, str]:
    """Map a controller result to the exact public (exit, stdout, stderr) triple."""
    if result:
        return (0, format_recovery_output(result), "")
    return (0, "", "")


def run_watchdog(
    paths: Paths,
    *,
    acquire: Callable[[Path], int | None],
    prepare: Callable[[Paths], ControllerSnapshot],
    healthy: Callable[[ControllerSnapshot], bool],
    bridge_only: Callable[[ControllerSnapshot], bool],
    full: Callable[[ControllerSnapshot], str],
) -> str:
    """Decide and run healthy, bridge-only, or full recovery under the lock."""
    lock = acquire(paths.lock)
    if lock is None:
        return ""
    try:
        snapshot = prepare(paths)
        if healthy(snapshot):
            return ""
        if bridge_only(snapshot):
            return ""
        return full(snapshot)
    finally:
        os.close(lock)


def run_cli(argv: Sequence[str]) -> int:
    """Run one of the two explicit execution roles."""
    try:
        command = parse_cli(argv)
        if command.role == "supervisor":
            assert command.nonce is not None
            result = supervise_tunnel(
                paths=Paths.for_root(command.state_root), nonce=command.nonce
            )
            if type(result) is not int or not 0 <= result <= 255:
                raise WatchdogError
            return result
        # Watchdog controller role: drive the controller under the lock and
        # render the exact public outcome.
        paths = Paths.for_root(command.state_root)
        result = run_watchdog(paths, **controller_actions(paths))
        exit_code, stdout, stderr = render_watchdog_result(result)
        if stdout:
            sys.stdout.write(stdout)
        if stderr:
            sys.stderr.write(stderr)
        return exit_code
    except Exception:  # noqa: BLE001 - top-level static-output boundary
        sys.stderr.write(FAILURE)
        return 1


if __name__ == "__main__":
    raise SystemExit(run_cli(sys.argv[1:]))
