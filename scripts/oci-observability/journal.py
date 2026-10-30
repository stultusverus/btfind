"""Bounded, forward-only journal reader for the two btfind services."""

import json
import os
import pwd
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone

from diagnostics import emit

UNIT_SERVICES = {
    "btfind.service": "btfind-crawler",
    "btfind-web.service": "btfind-web",
}

IDENTIFIER_SERVICES = {
    "btfind-crawler": "btfind-crawler",
    "btfind-web": "btfind-web",
}


class JournalError(Exception):
    pass


class JournalCursorError(JournalError):
    pass


@dataclass(frozen=True)
class JournalRecord:
    cursor: str
    timestamp: datetime
    service: str
    content: dict
    boot_id: str
    machine_id: str


@dataclass(frozen=True)
class JournalBatch:
    records: list
    cursor: str


def journal_uid():
    configured = os.environ.get("BTFIND_JOURNAL_UID")
    if configured:
        return int(configured)
    user = os.environ.get("BTFIND_INSTANCE_USER", "opc")
    try:
        return pwd.getpwnam(user).pw_uid
    except KeyError:
        return 1000


def decode_field(value, default=""):
    if value is None:
        return default
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, list):
        try:
            return bytes(value).decode("utf-8", "replace")
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def parse_content(message):
    if not message:
        return {"message": ""}
    try:
        parsed = json.loads(message)
    except (TypeError, json.JSONDecodeError):
        return {"message": message}
    if isinstance(parsed, dict):
        return parsed
    return {"message": message}


def parse_timestamp(value):
    try:
        microseconds = int(decode_field(value))
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(microseconds / 1_000_000, tz=timezone.utc)


def service_for(raw, uid):
    raw_uid = decode_field(raw.get("_UID"))
    if raw_uid and raw_uid != str(uid):
        return None

    unit = decode_field(raw.get("_SYSTEMD_USER_UNIT"))
    identifier = decode_field(raw.get("SYSLOG_IDENTIFIER"))

    if unit:
        service = UNIT_SERVICES.get(unit)
        if service is None:
            return None
        if identifier and IDENTIFIER_SERVICES.get(identifier) != service:
            return None
        return service

    return IDENTIFIER_SERVICES.get(identifier)


def normalise(raw, uid):
    cursor = decode_field(raw.get("__CURSOR"))
    if not cursor:
        return None
    service = service_for(raw, uid)
    if service is None:
        return None
    return JournalRecord(
        cursor=cursor,
        timestamp=parse_timestamp(raw.get("__REALTIME_TIMESTAMP")),
        service=service,
        content=parse_content(decode_field(raw.get("MESSAGE"))),
        boot_id=decode_field(raw.get("_BOOT_ID")),
        machine_id=decode_field(raw.get("_MACHINE_ID")),
    )


def build_command(cursor, lookback_seconds, uid):
    command = [
        "journalctl",
        "--system",
        "--no-pager",
        "--quiet",
        "--output=json",
        f"_UID={uid}",
        "_SYSTEMD_USER_UNIT=btfind.service",
        "_SYSTEMD_USER_UNIT=btfind-web.service",
    ]
    if cursor:
        command.append(f"--after-cursor={cursor}")
    elif lookback_seconds is not None:
        command.append(f"--since=-{int(lookback_seconds)}s")
    return command


def read_batches(
    cursor,
    lookback_seconds,
    max_batches,
    max_records,
    max_bytes,
):
    """Yield bounded batches in journal order.

    journalctl streams chronologically for --after-cursor and --since, so this
    never applies the latest-record (-n) semantics.  The reader stops after the
    batch bounds and commits no journal state itself.
    """

    uid = journal_uid()
    command = build_command(cursor, lookback_seconds, uid)
    stderr_file = tempfile.TemporaryFile(mode="w+", encoding="utf-8")
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=stderr_file,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    emitted = 0
    batch = []
    batch_bytes = 0
    batch_cursor = cursor
    last_raw_cursor = cursor
    terminated = False

    try:
        assert process.stdout is not None
        for line in process.stdout:
            if not line.strip():
                continue
            encoded_size = len(line.encode("utf-8", "replace"))
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as error:
                emit(
                    "journal_record_invalid",
                    level="warning",
                    reason="json",
                    error=str(error),
                )
                continue

            raw_cursor = decode_field(raw.get("__CURSOR"))
            if not raw_cursor:
                continue
            last_raw_cursor = raw_cursor
            record = normalise(raw, uid)
            if record is None:
                continue

            if batch and (
                len(batch) >= max_records or batch_bytes + encoded_size > max_bytes
            ):
                yield JournalBatch(batch, batch_cursor)
                emitted += 1
                if emitted >= max_batches:
                    terminated = True
                    break
                batch = []
                batch_bytes = 0
                batch_cursor = cursor

            batch.append(record)
            batch_bytes += encoded_size
            batch_cursor = raw_cursor

        if not terminated and (batch or last_raw_cursor != cursor):
            yield JournalBatch(batch, last_raw_cursor)
    finally:
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        stderr_file.seek(0)
        stderr = stderr_file.read()
        stderr_file.close()

    if not terminated and process.returncode not in (0, -15):
        if "Failed to seek" in stderr or "Invalid cursor" in stderr:
            raise JournalCursorError(stderr.strip() or "invalid journal cursor")
        raise JournalError(stderr.strip() or f"journalctl exited with {process.returncode}")
