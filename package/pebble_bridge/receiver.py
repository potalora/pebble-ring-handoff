from __future__ import annotations

import hmac
import ipaddress
import logging
import os
import re
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from starlette.applications import Starlette
from starlette.datastructures import UploadFile
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from .repository import Repository

REQUEST_LIMIT = 9 * 1024 * 1024
AUDIO_LIMIT = 8 * 1024 * 1024
TRANSCRIPT_LIMIT = 4_000
FUTURE_SKEW_MS = 5 * 60 * 1000
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
PART_NAMES = frozenset({"audio", "transcription", "recordedAt", "client"})
TEST_EVENT_PART_NAMES = frozenset({"client", "recordedAt", "transcription", "test"})
NEW_TOPIC = re.compile(r"^\s*(?:new topic|new conversation)\b", re.IGNORECASE)
MP4_BRANDS = (b"M4A ", b"isom", b"iso2", b"mp41", b"mp42", b"avc1")
LOGGER = logging.getLogger(__name__)
SAFE_REJECTION_REASONS = frozenset({
    "forbidden", "unauthorized", "multipart required", "invalid content length", "request too large",
    "invalid multipart", "invalid parts", "missing parts", "invalid client", "invalid recordedAt",
    "invalid transcription", "transcription too long", "unsupported audio", "invalid audio size",
})


async def pebble_http_exception(request: Request, exc: HTTPException) -> Response:
    """Log only a static rejection class; never request payloads or headers."""
    if request.url.path == "/pebble":
        reason = exc.detail if isinstance(exc.detail, str) and exc.detail in SAFE_REJECTION_REASONS else "rejected"
        LOGGER.warning("Pebble webhook rejected request: status=%d reason=%s", exc.status_code, reason)
    return JSONResponse({"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers)


@dataclass(frozen=True)
class ReceiverSettings:
    data_dir: Path
    webhook_token: str | None
    allowed_hosts: frozenset[str]
    port: int = 8765

    def __post_init__(self):
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError('invalid receiver port')

    @classmethod
    def from_env(cls):
        return cls(data_dir=Path(os.environ['PEBBLE_DATA_DIR']),
            webhook_token=os.environ['PEBBLE_WEBHOOK_TOKEN'],
            allowed_hosts=frozenset(os.environ['PEBBLE_ALLOWED_HOSTS'].split(',')),
            port=parse_port(os.getenv('PORT', '8765')))


class RequestTooLarge(Exception):
    pass


def parse_port(value: str) -> int:
    if re.fullmatch(r"[0-9]+", value) is None:
        raise ValueError("PORT must be an integer from 1 through 65535")
    port = int(value)
    if not 1 <= port <= 65535:
        raise ValueError("PORT must be an integer from 1 through 65535")
    return port


def parse_nonnegative_ms(value: str) -> int:
    if re.fullmatch(r"[0-9]+", value) is None:
        raise ValueError("retention values must be non-negative integer milliseconds")
    return int(value)


def _valid_hostname(value: str) -> bool:
    if value == "localhost":
        return True
    try:
        ipaddress.IPv4Address(value)
        return True
    except ValueError:
        pass
    if len(value) > 253 or value.endswith("."):
        return False
    return all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in value.split("."))


def parse_host_header(value: str) -> str | None:
    """Parse only valid HTTP Host forms; never silently discard a bad port."""
    value = value.strip().lower()
    if not value:
        return None
    if value.startswith("["):
        matched = re.fullmatch(r"\[([^][]+)\](?::([0-9]+))?", value)
        if matched is None:
            return None
        host, port = matched.groups()
        try:
            ipaddress.IPv6Address(host)
        except ValueError:
            return None
        if port is not None:
            try:
                parse_port(port)
            except ValueError:
                return None
        return host
    if "[" in value or "]" in value or value.count(":") > 1:
        return None
    host, separator, port = value.partition(":")
    if separator:
        try:
            parse_port(port)
        except ValueError:
            return None
    return host if _valid_hostname(host) else None


def _is_mp4_or_m4a(audio: bytes) -> bool:
    return len(audio) >= 12 and audio[4:8] == b"ftyp" and any(brand in audio[8:32] for brand in MP4_BRANDS)


def create_receiver(
    settings: ReceiverSettings,
    *,
    now: Callable[[], int] | None = None,
) -> Starlette:
    repository = Repository(settings.data_dir)
    clock = now or (lambda: int(time.time() * 1000))
    async def pebble(request: Request) -> Response:
        if parse_host_header(request.headers.get("host", "")) not in LOOPBACK_HOSTS | settings.allowed_hosts:
            raise HTTPException(403, "forbidden")
        expected = f"Bearer {settings.webhook_token}" if settings.webhook_token else ""
        supplied = request.headers.get("authorization", "")
        if not expected or not hmac.compare_digest(supplied, expected):
            raise HTTPException(401, "unauthorized")
        content_type = request.headers.get("content-type", "")
        if not content_type.lower().startswith("multipart/form-data"):
            raise HTTPException(415, "multipart required")
        content_length = request.headers.get("content-length")
        if content_length is not None:
            if re.fullmatch(r"[0-9]+", content_length) is None:
                raise HTTPException(400, "invalid content length")
            if int(content_length) > REQUEST_LIMIT:
                raise HTTPException(413, "request too large")

        received = 0
        original_receive = request.receive

        async def limited_receive() -> dict:
            nonlocal received
            message = await original_receive()
            body = message.get("body", b"")
            received += len(body)
            if received > REQUEST_LIMIT:
                raise RequestTooLarge
            return message

        limited_request = Request(request.scope, limited_receive)
        try:
            form = await limited_request.form(max_files=2, max_fields=4)
        except RequestTooLarge:
            raise HTTPException(413, "request too large") from None
        except Exception:  # noqa: BLE001 - normalize multipart parser failures
            raise HTTPException(400, "invalid multipart") from None
        items = list(form.multi_items())
        names = [name for name, _ in items]
        if len(names) != len(set(names)):
            raise HTTPException(400, "invalid parts")
        values = dict(items)
        has_audio = "audio" in names
        has_transcription = "transcription" in names
        if (
            not has_audio
            and frozenset(names) == TEST_EVENT_PART_NAMES
            and all(isinstance(values.get(name), str) for name in TEST_EVENT_PART_NAMES)
            and values.get("test") == "true"
            and request.headers.get("x-index-test") == "true"
            and request.headers.get("x-index-trigger") == "test-event"
        ):
            return Response(status_code=204)
        if set(names) - PART_NAMES:
            raise HTTPException(400, "invalid parts")
        if not {"client", "recordedAt"}.issubset(names):
            raise HTTPException(400, "missing parts")
        values = dict(items)
        if not isinstance(values["client"], str) or values["client"] != "ring":
            raise HTTPException(400, "invalid client")
        recorded_value = values["recordedAt"]
        if not isinstance(recorded_value, str) or re.fullmatch(r"[0-9]+", recorded_value) is None:
            raise HTTPException(400, "invalid recordedAt")
        recorded_at = int(recorded_value)
        if recorded_at > clock() + FUTURE_SKEW_MS:
            raise HTTPException(400, "invalid recordedAt")

        if not has_audio or not has_transcription:
            raise HTTPException(400, "missing parts")

        transcript_value = values.get("transcription")
        if transcript_value is None or not isinstance(transcript_value, str) or not transcript_value.strip():
            raise HTTPException(400, "invalid transcription")
        if len(transcript_value) > TRANSCRIPT_LIMIT:
            raise HTTPException(413, "transcription too long")
        audio_value = values.get("audio")
        audio: bytes | None = None
        if audio_value is not None:
            if not isinstance(audio_value, UploadFile) or audio_value.content_type != "audio/mp4":
                raise HTTPException(415, "unsupported audio")
            size_header = request.headers.get("x-audio-size")
            if size_header is None or re.fullmatch(r"[0-9]+", size_header) is None:
                raise HTTPException(400, "invalid audio size")
            audio = await audio_value.read()
            if len(audio) > AUDIO_LIMIT:
                raise HTTPException(413, "audio too large")
            if int(size_header) != len(audio):
                raise HTTPException(400, "invalid audio size")
            if not _is_mp4_or_m4a(audio):
                raise HTTPException(415, "unsupported audio")
        elif request.headers.get("x-audio-size") is not None:
            raise HTTPException(400, "invalid audio size")
        _capture, inserted = repository.persist_capture(
            client="ring", recorded_at=recorded_at, received_at=clock(), transcript=transcript_value,
            audio=audio, forced_new_topic=bool(transcript_value and NEW_TOPIC.search(transcript_value)),
        )
        return JSONResponse({"accepted": True, "duplicate": not inserted}, status_code=202)

    @asynccontextmanager
    async def lifespan(app: Starlette):
        try:
            yield
        finally:
            repository.connection.close()

    app = Starlette(
        routes=[Route("/pebble", pebble, methods=["POST"])],
        lifespan=lifespan,
        exception_handlers={HTTPException: pebble_http_exception},
    )
    app.state.repository = repository
    app.state.settings = settings
    return app
