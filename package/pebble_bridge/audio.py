"""Explicit local audio retrieval; no upload, model call, or re-transcription."""
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .relay_store import RelayStore


@dataclass(frozen=True)
class Recording:
    path: Path
    data: bytes
    expires_at: int


def retrieve_audio(store: RelayStore, capture_id: int, *, identity: dict,
                   now_ms: Callable[[], int], kind: str = 'retrieve') -> Recording:
    record = store.claim_audio(capture_id, **identity, kind=kind, now_ms=now_ms())
    path = Path(record['audio_path'])
    spool = store.data_dir / 'spool'
    name = str(record['fingerprint']) + '.m4a'
    size = record['audio_size']
    if (not re.fullmatch(r'[0-9a-f]{64}\.m4a', name) or path != spool / name
            or type(size) is not int or not 0 < size <= 32 * 1024 * 1024):
        raise PermissionError('audio expired or unavailable')
    directory = file = None
    try:
        directory = os.open(spool, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        ds = os.fstat(directory)
        if ds.st_uid != os.getuid() or stat.S_IMODE(ds.st_mode) & 0o077:
            raise PermissionError('audio expired or unavailable')
        file = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        before = os.fstat(file)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or stat.S_IMODE(before.st_mode) & 0o077 or before.st_nlink != 1
                or before.st_size != size):
            raise PermissionError('audio expired or unavailable')
        chunks = []
        remaining = size + 1
        while remaining:
            chunk = os.read(file, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b''.join(chunks)
        after = os.fstat(file)
        identity_fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns', 'st_nlink')
        if (len(data) != size or now_ms() >= record['audio_expires_at']
                or any(getattr(before, name) != getattr(after, name) for name in identity_fields)):
            raise PermissionError('audio expired or unavailable')
        return Recording(path, data, record['audio_expires_at'])
    except OSError as error:
        raise PermissionError('audio expired or unavailable') from error
    finally:
        if file is not None:
            os.close(file)
        if directory is not None:
            os.close(directory)



def purge_expired_audio(store: RelayStore, *, scope: dict, now_ms: int) -> int:
    """Delete only expired, scoped spool entries; preserve transcripts/decisions.

    Unlink before clearing the database pointer so interrupted cleanup is safe
    to repeat. Missing files represent completed/externally removed recordings.
    """
    records = store.expired_audio(**scope, now_ms=now_ms)
    if not records:
        return 0
    spool = store.data_dir / 'spool'
    directory = os.open(spool, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(directory)
        if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise PermissionError('unsafe recording spool')
        for record in records:
            name = str(record['fingerprint']) + '.m4a'
            if (not re.fullmatch(r'[0-9a-f]{64}\.m4a', name)
                    or Path(record['audio_path']) != spool/name):
                raise PermissionError('unsafe recording path')
            try:
                os.unlink(name, dir_fd=directory)
            except FileNotFoundError:
                pass
            store.forget_audio(record['id'], record['audio_path'])
        return len(records)
    finally:
        os.close(directory)
