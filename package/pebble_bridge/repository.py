from __future__ import annotations

import hashlib
import os
import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final

MAX_DELIVERY_ATTEMPTS: Final = 10
HANDOFF_FIELD_MAX_CHARS: Final = 4_000
SCHEMA_VERSION: Final = 2


def _migrate_approval_schema(
    connection: sqlite3.Connection, *, schema_version: int
) -> None:
    """Atomically migrate approval columns, indexes, state, and version."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(capture_approvals)")
        }
        for name, definition in (
            ("guild_id", "TEXT NOT NULL DEFAULT ''"),
            ("card_claimed_at", "INTEGER"),
            ("approver_user_id", "TEXT NOT NULL DEFAULT ''"),
            ("approval_post_state", "TEXT NOT NULL DEFAULT 'ready'"),
            ("nonce_hash", "TEXT"),
            ("approval_message_id", "TEXT"),
            ("decided_by", "TEXT"),
            ("decided_at", "INTEGER"),
            ("dispatch_state", "TEXT NOT NULL DEFAULT 'not_dispatched'"),
            ("dispatch_started_at", "INTEGER"),
            ("dispatch_completed_at", "INTEGER"),
            ("last_error", "TEXT"),
        ):
            if name not in existing_columns:
                connection.execute(
                    f"ALTER TABLE capture_approvals ADD COLUMN {name} {definition}"
                )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS capture_approvals_message_unique
            ON capture_approvals(approval_message_id)
            WHERE approval_message_id IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS capture_approvals_pending
            ON capture_approvals(
                channel_id, status, approval_post_state, capture_id
            )
            """
        )
        if schema_version == 0:
            connection.execute(
                """
                UPDATE captures
                SET state = 'manual',
                    last_error = 'legacy capture requires reconciliation',
                    terminal_at = ?
                WHERE state = 'pending'
                  AND (
                      discord_uploaded != 0
                      OR thread_id IS NOT NULL
                      OR hermes_session_id IS NOT NULL
                      OR answer_text IS NOT NULL
                      OR capture_post_token IS NOT NULL
                      OR answer_post_token IS NOT NULL
                  )
                """,
                (int(time.time() * 1000),),
            )
        if schema_version < 2:
            now_ms = int(time.time() * 1000)
            legacy_claimed = [
                int(row[0])
                for row in connection.execute(
                    """
                    SELECT capture_id FROM capture_approvals
                    WHERE card_claimed_at IS NOT NULL AND approver_user_id = ''
                    """
                ).fetchall()
            ]
            if legacy_claimed:
                placeholders = ",".join("?" for _ in legacy_claimed)
                connection.execute(
                    f"""
                    UPDATE capture_approvals
                    SET status = 'migration_quarantined',
                        approval_post_state = 'uncertain',
                        dispatch_state = CASE
                            WHEN dispatch_state = 'dispatching' THEN 'uncertain'
                            ELSE dispatch_state
                        END,
                        last_error = 'legacy approval missing approver binding'
                    WHERE capture_id IN ({placeholders})
                    """,
                    legacy_claimed,
                )
                connection.execute(
                    f"""
                    UPDATE captures
                    SET state = 'manual', terminal_at = ?,
                        last_error = 'legacy approval missing approver binding'
                    WHERE id IN ({placeholders})
                    """,
                    (now_ms, *legacy_claimed),
                )
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise


@dataclass(frozen=True)
class Capture:
    id: int
    fingerprint: str
    recorded_at: int
    received_at: int
    transcript: str | None
    audio_path: Path | None
    audio_size: int | None
    state: str
    thread_id: str | None
    hermes_session_id: str | None
    discord_uploaded: bool
    answer_text: str | None
    attempts: int
    next_attempt_at: int
    forced_new_topic: bool
    capture_post_token: str | None
    answer_post_token: str | None
    terminal_at: int | None


@dataclass(frozen=True)
class RelayHandoffContext:
    thread_id: str
    transcript: str | None
    relay_answer: str | None


@dataclass(frozen=True)
class CaptureApproval:
    capture_id: int
    channel_id: str
    capture_message_id: str
    transcript_sha256: str
    status: str
    expires_at: int


class Repository:
    """SQLite-backed durable inbox/outbox. It intentionally stores no credentials."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.spool_dir = data_dir / "spool"
        self.data_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.spool_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.data_dir, 0o700)
        os.chmod(self.spool_dir, 0o700)
        self.db_path = data_dir / "bridge.sqlite3"
        self.connection = sqlite3.connect(self.db_path, isolation_level=None, check_same_thread=False)
        os.chmod(self.db_path, 0o600)
        self.connection.row_factory = sqlite3.Row
        schema_version = int(
            self.connection.execute("PRAGMA user_version").fetchone()[0]
        )
        if schema_version not in {0, 1, SCHEMA_VERSION}:
            self.connection.close()
            raise RuntimeError(
                f"unsupported bridge schema version: {schema_version}"
            )
        self.connection.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS captures (
                id INTEGER PRIMARY KEY,
                fingerprint TEXT NOT NULL UNIQUE,
                recorded_at INTEGER NOT NULL,
                received_at INTEGER NOT NULL,
                transcript TEXT,
                audio_path TEXT,
                audio_size INTEGER,
                state TEXT NOT NULL DEFAULT 'pending',
                thread_id TEXT,
                hermes_session_id TEXT,
                discord_uploaded INTEGER NOT NULL DEFAULT 0,
                answer_text TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER NOT NULL,
                forced_new_topic INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                capture_post_token TEXT,
                answer_post_token TEXT,
                terminal_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS captures_due ON captures(state, next_attempt_at, id);
            CREATE INDEX IF NOT EXISTS captures_thread_received ON captures(thread_id, received_at, id);
            CREATE TABLE IF NOT EXISTS threads (
                thread_id TEXT PRIMARY KEY,
                hermes_session_id TEXT,
                last_capture_id INTEGER NOT NULL,
                last_received_at INTEGER NOT NULL,
                owner TEXT NOT NULL DEFAULT 'relay',
                handed_off_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS thread_delivery_reservations (
                thread_id TEXT PRIMARY KEY,
                capture_id INTEGER NOT NULL UNIQUE,
                FOREIGN KEY(thread_id) REFERENCES threads(thread_id),
                FOREIGN KEY(capture_id) REFERENCES captures(id)
            );
            CREATE TABLE IF NOT EXISTS capture_approvals (
                capture_id INTEGER PRIMARY KEY,
                channel_id TEXT NOT NULL,
                guild_id TEXT NOT NULL DEFAULT '',
                card_claimed_at INTEGER,
                approver_user_id TEXT NOT NULL DEFAULT '',
                capture_message_id TEXT NOT NULL UNIQUE,
                transcript_sha256 TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                expires_at INTEGER NOT NULL,
                approval_post_state TEXT NOT NULL DEFAULT 'ready',
                nonce_hash TEXT,
                approval_message_id TEXT UNIQUE,
                decided_by TEXT,
                decided_at INTEGER,
                dispatch_state TEXT NOT NULL DEFAULT 'not_dispatched',
                dispatch_started_at INTEGER,
                dispatch_completed_at INTEGER,
                last_error TEXT,
                FOREIGN KEY(capture_id) REFERENCES captures(id)
            );
            """
        )
        existing_columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(captures)")}
        for name, definition in (
            ("capture_post_token", "TEXT"),
            ("answer_post_token", "TEXT"),
            ("terminal_at", "INTEGER"),
        ):
            if name not in existing_columns:
                self.connection.execute(f"ALTER TABLE captures ADD COLUMN {name} {definition}")
        existing_thread_columns = {
            row["name"] for row in self.connection.execute("PRAGMA table_info(threads)")
        }
        for name, definition in (
            ("owner", "TEXT NOT NULL DEFAULT 'relay'"),
            ("handed_off_at", "INTEGER"),
        ):
            if name not in existing_thread_columns:
                self.connection.execute(f"ALTER TABLE threads ADD COLUMN {name} {definition}")
        try:
            _migrate_approval_schema(self.connection, schema_version=schema_version)
        except BaseException:
            self.connection.close()
            raise
        for suffix in ("-wal", "-shm"):
            path = Path(f"{self.db_path}{suffix}")
            if path.exists():
                os.chmod(path, 0o600)
        self.sweep_orphan_spool()

    def sweep_orphan_spool(self) -> int:
        """Delete local audio with no durable capture reference after a crash."""
        referenced = {
            Path(row["audio_path"]).resolve()
            for row in self.connection.execute("SELECT audio_path FROM captures WHERE audio_path IS NOT NULL")
        }
        deleted = 0
        for path in self.spool_dir.iterdir():
            try:
                if path.is_file() and path.resolve() not in referenced:
                    path.unlink()
                    deleted += 1
            except FileNotFoundError:
                continue
        return deleted

    @staticmethod
    def fingerprint(client: str, recorded_at: int, transcript: str | None, audio: bytes | None) -> str:
        audio_sha = hashlib.sha256(audio or b"").hexdigest()
        value = "|".join((client, str(recorded_at), transcript or "", audio_sha))
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _spool_audio(self, fingerprint: str, audio: bytes) -> Path:
        final = self.spool_dir / f"{fingerprint}.m4a"
        if final.exists():
            return final
        temp = self.spool_dir / f".{fingerprint}.{os.getpid()}.tmp"
        try:
            fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                offset = 0
                while offset < len(audio):
                    offset += os.write(fd, audio[offset:])
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(temp, final)
            os.chmod(final, 0o600)
            directory_fd = os.open(self.spool_dir, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return final
        finally:
            if temp.exists():
                temp.unlink(missing_ok=True)

    def persist_capture(
        self,
        *,
        client: str,
        recorded_at: int,
        received_at: int,
        transcript: str | None,
        audio: bytes | None,
        forced_new_topic: bool,
    ) -> tuple[Capture, bool]:
        fingerprint = self.fingerprint(client, recorded_at, transcript, audio)
        spool_path = self._spool_audio(fingerprint, audio) if audio is not None else None
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO captures(
                    fingerprint, recorded_at, received_at, transcript, audio_path, audio_size,
                    next_attempt_at, forced_new_topic
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fingerprint,
                    recorded_at,
                    received_at,
                    transcript,
                    str(spool_path) if spool_path else None,
                    len(audio) if audio is not None else None,
                    received_at,
                    int(forced_new_topic),
                ),
            )
            inserted = cursor.rowcount == 1
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            if spool_path is not None:
                spool_path.unlink(missing_ok=True)
            raise
        row = self.connection.execute("SELECT * FROM captures WHERE fingerprint = ?", (fingerprint,)).fetchone()
        assert row is not None
        if not inserted and spool_path is not None and row["audio_path"] != str(spool_path):
            spool_path.unlink(missing_ok=True)
        return self._capture(row), inserted

    def capture_count(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM captures").fetchone()[0])

    def next_due(self, *, now_ms: int | None = None) -> Capture | None:
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        row = self.connection.execute(
            "SELECT * FROM captures WHERE state = 'pending' AND next_attempt_at <= ? ORDER BY id LIMIT 1",
            (now_ms,),
        ).fetchone()
        return self._capture(row) if row else None

    def get_capture(self, capture_id: int) -> Capture | None:
        row = self.connection.execute("SELECT * FROM captures WHERE id = ?", (capture_id,)).fetchone()
        return self._capture(row) if row else None

    def mark_awaiting_approval(
        self,
        capture_id: int,
        *,
        channel_id: str,
        capture_message_id: str,
        now_ms: int,
        ttl_ms: int,
    ) -> CaptureApproval:
        if not isinstance(ttl_ms, int) or isinstance(ttl_ms, bool) or ttl_ms <= 0:
            raise ValueError("approval ttl must be a positive integer")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            capture = self.connection.execute(
                "SELECT transcript FROM captures WHERE id = ? AND state = 'pending'",
                (capture_id,),
            ).fetchone()
            if capture is None:
                raise RuntimeError("capture cannot await approval")
            transcript = capture["transcript"] or ""
            transcript_sha256 = hashlib.sha256(transcript.encode("utf-8")).hexdigest()
            self.connection.execute(
                """
                INSERT INTO capture_approvals(
                    capture_id, channel_id, capture_message_id,
                    transcript_sha256, expires_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    capture_id,
                    channel_id,
                    capture_message_id,
                    transcript_sha256,
                    now_ms + ttl_ms,
                ),
            )
            self.connection.execute(
                """
                UPDATE captures
                SET state = 'awaiting_approval', discord_uploaded = 1,
                    capture_post_token = NULL
                WHERE id = ?
                """,
                (capture_id,),
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        approval = self.get_approval(capture_id)
        assert approval is not None
        return approval

    def get_approval(self, capture_id: int) -> CaptureApproval | None:
        row = self.connection.execute(
            "SELECT * FROM capture_approvals WHERE capture_id = ?", (capture_id,)
        ).fetchone()
        if row is None:
            return None
        return CaptureApproval(
            capture_id=row["capture_id"],
            channel_id=row["channel_id"],
            capture_message_id=row["capture_message_id"],
            transcript_sha256=row["transcript_sha256"],
            status=row["status"],
            expires_at=row["expires_at"],
        )

    def _capture(self, row: sqlite3.Row) -> Capture:
        return Capture(
            id=row["id"], fingerprint=row["fingerprint"], recorded_at=row["recorded_at"],
            received_at=row["received_at"], transcript=row["transcript"],
            audio_path=Path(row["audio_path"]) if row["audio_path"] else None,
            audio_size=row["audio_size"], state=row["state"], thread_id=row["thread_id"],
            hermes_session_id=row["hermes_session_id"], discord_uploaded=bool(row["discord_uploaded"]),
            answer_text=row["answer_text"], attempts=row["attempts"], next_attempt_at=row["next_attempt_at"],
            forced_new_topic=bool(row["forced_new_topic"]), capture_post_token=row["capture_post_token"],
            answer_post_token=row["answer_post_token"], terminal_at=row["terminal_at"],
        )

    def immediate_preceding_route(self, capture: Capture, *, followup: bool, window_ms: int) -> tuple[str, str | None] | None:
        """Return a route only for a mechanical follow-up to the previous arrival."""
        if capture.forced_new_topic or not followup:
            return None
        previous = self.connection.execute(
            """
            SELECT thread_id, forced_new_topic FROM captures
            WHERE (received_at < ? OR (received_at = ? AND id < ?))
            ORDER BY received_at DESC, id DESC LIMIT 1
            """,
            (capture.received_at, capture.received_at, capture.id),
        ).fetchone()
        if not previous or previous["forced_new_topic"] or not previous["thread_id"]:
            return None
        last = self.connection.execute(
            """
            SELECT last_received_at, hermes_session_id FROM threads
            WHERE thread_id = ? AND owner = 'relay'
            """,
            (previous["thread_id"],),
        ).fetchone()
        if not last or capture.received_at - int(last["last_received_at"]) > window_ms:
            return None
        return str(previous["thread_id"]), last["hermes_session_id"]

    def reserve_immediate_preceding_route(
        self, capture: Capture, *, followup: bool, window_ms: int
    ) -> tuple[str, str | None] | None:
        """Atomically reserve a Relay route before its Discord delivery begins."""
        if capture.forced_new_topic or not followup:
            return None
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            previous = self.connection.execute(
                """
                SELECT thread_id, forced_new_topic FROM captures
                WHERE (received_at < ? OR (received_at = ? AND id < ?))
                ORDER BY received_at DESC, id DESC LIMIT 1
                """,
                (capture.received_at, capture.received_at, capture.id),
            ).fetchone()
            if not previous or previous["forced_new_topic"] or not previous["thread_id"]:
                self.connection.execute("COMMIT")
                return None
            last = self.connection.execute(
                """
                SELECT last_received_at, hermes_session_id FROM threads
                WHERE thread_id = ? AND owner = 'relay'
                """,
                (previous["thread_id"],),
            ).fetchone()
            if not last or capture.received_at - int(last["last_received_at"]) > window_ms:
                self.connection.execute("COMMIT")
                return None
            reserved = self.connection.execute(
                """
                INSERT OR IGNORE INTO thread_delivery_reservations(thread_id, capture_id)
                VALUES (?, ?)
                """,
                (previous["thread_id"], capture.id),
            )
            if reserved.rowcount != 1:
                self.connection.execute("COMMIT")
                return None
            self.connection.execute("COMMIT")
            return str(previous["thread_id"]), last["hermes_session_id"]
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def mark_uploaded(self, capture_id: int, *, thread_id: str, hermes_session_id: str | None) -> None:
        row = self.connection.execute("SELECT received_at FROM captures WHERE id = ?", (capture_id,)).fetchone()
        if not row:
            raise KeyError(capture_id)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            reservation = self.connection.execute(
                "SELECT thread_id FROM thread_delivery_reservations WHERE capture_id = ?", (capture_id,)
            ).fetchone()
            if reservation is not None and reservation["thread_id"] != thread_id:
                raise RuntimeError("reserved capture delivered to unexpected thread")
            self.connection.execute(
                "UPDATE captures SET thread_id = ?, hermes_session_id = ?, discord_uploaded = 1, capture_post_token = NULL WHERE id = ?",
                (thread_id, hermes_session_id, capture_id),
            )
            self.connection.execute(
                """
                INSERT INTO threads(thread_id, hermes_session_id, last_capture_id, last_received_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(thread_id) DO UPDATE SET
                    last_capture_id = excluded.last_capture_id,
                    last_received_at = excluded.last_received_at
                """,
                (thread_id, hermes_session_id, capture_id, row["received_at"]),
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def session_for_thread(self, thread_id: str) -> str | None:
        row = self.connection.execute("SELECT hermes_session_id FROM threads WHERE thread_id = ?", (thread_id,)).fetchone()
        return row["hermes_session_id"] if row and row["hermes_session_id"] else None

    def claim_thread_for_hermes(
        self, thread_id: str, *, handed_off_at: int
    ) -> RelayHandoffContext | None:
        """Claim a Relay-owned thread once and return text-only handoff context."""
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            claimed = self.connection.execute(
                """
                UPDATE threads SET owner = 'hermes', handed_off_at = ?
                WHERE thread_id = ?
                  AND owner = 'relay'
                  AND NOT EXISTS (
                      SELECT 1 FROM thread_delivery_reservations
                      WHERE thread_id = threads.thread_id
                  )
                """,
                (handed_off_at, thread_id),
            )
            if claimed.rowcount != 1:
                self.connection.execute("COMMIT")
                return None
            row = self.connection.execute(
                """
                SELECT captures.transcript, captures.answer_text
                FROM threads
                JOIN captures ON captures.id = threads.last_capture_id
                WHERE threads.thread_id = ?
                """,
                (thread_id,),
            ).fetchone()
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        if row is None:
            return RelayHandoffContext(thread_id, None, None)
        transcript = row["transcript"]
        relay_answer = row["answer_text"]
        return RelayHandoffContext(
            thread_id,
            transcript[:HANDOFF_FIELD_MAX_CHARS] if transcript is not None else None,
            relay_answer[:HANDOFF_FIELD_MAX_CHARS] if relay_answer is not None else None,
        )

    def save_session(self, capture_id: int, *, thread_id: str, session_id: str) -> None:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute("UPDATE captures SET hermes_session_id = ? WHERE id = ?", (session_id, capture_id))
            self.connection.execute("UPDATE threads SET hermes_session_id = ? WHERE thread_id = ?", (session_id, thread_id))
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def store_answer(self, capture_id: int, answer: str) -> None:
        self.connection.execute("UPDATE captures SET answer_text = ? WHERE id = ?", (answer, capture_id))

    def begin_capture_post(self, capture_id: int) -> str:
        return self._begin_discord_operation(capture_id, "capture_post_token")

    def begin_answer_post(self, capture_id: int) -> str:
        return self._begin_discord_operation(capture_id, "answer_post_token")

    def _begin_discord_operation(self, capture_id: int, column: str) -> str:
        token = secrets.token_urlsafe(24)
        cursor = self.connection.execute(
            f"UPDATE captures SET {column} = ? WHERE id = ? AND state = 'pending' AND {column} IS NULL",
            (token, capture_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("discord operation cannot be started")
        return token

    def clear_capture_post(self, capture_id: int) -> None:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute("DELETE FROM thread_delivery_reservations WHERE capture_id = ?", (capture_id,))
            self.connection.execute("UPDATE captures SET capture_post_token = NULL WHERE id = ?", (capture_id,))
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def clear_answer_post(self, capture_id: int) -> None:
        self.connection.execute("UPDATE captures SET answer_post_token = NULL WHERE id = ?", (capture_id,))

    def mark_done(self, capture_id: int, *, now_ms: int | None = None) -> None:
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute(
                "UPDATE captures SET state = 'done', last_error = NULL, answer_post_token = NULL, terminal_at = ? WHERE id = ?",
                (now_ms, capture_id),
            )
            self.connection.execute("DELETE FROM thread_delivery_reservations WHERE capture_id = ?", (capture_id,))
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def clear_audio_path(self, capture_id: int) -> None:
        self.connection.execute("UPDATE captures SET audio_path = NULL WHERE id = ?", (capture_id,))

    def schedule_retry(self, capture_id: int, *, now_ms: int) -> None:
        row = self.connection.execute("SELECT attempts FROM captures WHERE id = ?", (capture_id,)).fetchone()
        attempts = int(row["attempts"]) + 1
        if attempts >= MAX_DELIVERY_ATTEMPTS:
            self.mark_manual(capture_id, reason="delivery retry exhausted", now_ms=now_ms)
            return
        delay_ms = min(3_600_000, (2 ** min(attempts, 12)) * 1000)
        self.connection.execute(
            "UPDATE captures SET attempts = ?, next_attempt_at = ?, last_error = 'delivery failed' WHERE id = ?",
            (attempts, now_ms + delay_ms, capture_id),
        )

    def mark_dlq(self, capture_id: int, *, now_ms: int | None = None) -> None:
        self.mark_manual(capture_id, reason="ambiguous new thread", now_ms=now_ms, state="dlq")

    def mark_manual(self, capture_id: int, *, reason: str, now_ms: int | None = None, state: str = "manual") -> None:
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        self.connection.execute(
            "UPDATE captures SET state = ?, last_error = ?, terminal_at = ? WHERE id = ?",
            (state, reason, now_ms, capture_id),
        )

    def purge_retention(self, *, now_ms: int, completed_retention_ms: int, failed_retention_ms: int) -> int:
        if completed_retention_ms < 0 or failed_retention_ms < 0:
            raise ValueError("retention values must not be negative")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            rows = self.connection.execute(
                """
                SELECT id, audio_path FROM captures
                WHERE (state = 'done' AND terminal_at IS NOT NULL AND terminal_at <= ?)
                   OR (state IN ('dlq', 'manual', 'rejected', 'expired')
                       AND terminal_at IS NOT NULL AND terminal_at <= ?)
                """,
                (now_ms - completed_retention_ms, now_ms - failed_retention_ms),
            ).fetchall()
            for row in rows:
                self.connection.execute(
                    "DELETE FROM thread_delivery_reservations WHERE capture_id = ?", (row["id"],)
                )
                self.connection.execute(
                    "DELETE FROM capture_approvals WHERE capture_id = ?", (row["id"],)
                )
            for row in rows:
                if row["audio_path"]:
                    Path(row["audio_path"]).unlink(missing_ok=True)
                self.connection.execute("DELETE FROM captures WHERE id = ?", (row["id"],))
            self.connection.execute("DELETE FROM threads WHERE last_capture_id NOT IN (SELECT id FROM captures)")
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        return len(rows)
