"""Atomic, durable exporter checkpoints.

Logs and metrics have independent cursors.  A cursor only advances after the
corresponding destination acknowledged every non-empty partition in a batch.
"""

import copy
import fcntl
import json
import os
import time

from diagnostics import emit

STATE_VERSION = 1
MAX_GAPS = 10


def default_state():
    return {
        "version": STATE_VERSION,
        "logs": {
            "cursor": None,
            "initialized": False,
            "last_success_unix": None,
            "gaps": [],
        },
        "metrics": {
            "cursor": None,
            "initialized": False,
            "last_success_unix": None,
            "baselines": {},
            "gaps": [],
        },
    }


def _merge_defaults(loaded):
    state = default_state()
    if not isinstance(loaded, dict):
        return state
    for stream in ("logs", "metrics"):
        value = loaded.get(stream)
        if isinstance(value, dict):
            state[stream].update(value)
    return state


class CheckpointStore:
    def __init__(self, directory):
        self.directory = directory
        self.path = os.path.join(directory, "state.json")
        self.lock_path = os.path.join(directory, ".lock")
        self.state = None
        self._lock_fd = None

    def __enter__(self):
        os.makedirs(self.directory, mode=0o700, exist_ok=True)
        try:
            os.chmod(self.directory, 0o700)
        except OSError:
            pass
        self._lock_fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self._lock_fd)
            self._lock_fd = None
            raise RuntimeError("another btfind observability exporter is running")
        self.state = self._load()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._lock_fd is not None:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)
            self._lock_fd = None
        return False

    def _load(self):
        if not os.path.exists(self.path):
            return default_state()
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
        except (OSError, json.JSONDecodeError) as error:
            self._move_corrupt(error)
            return default_state()

        if not isinstance(loaded, dict) or loaded.get("version") != STATE_VERSION:
            self._move_corrupt("unsupported state version")
            return default_state()
        return _merge_defaults(loaded)

    def _move_corrupt(self, reason):
        backup = f"{self.path}.corrupt.{int(time.time())}"
        try:
            os.replace(self.path, backup)
            emit(
                "checkpoint_recovered",
                level="warning",
                reason="corrupt",
                backup=backup,
                error=str(reason),
            )
        except OSError as error:
            emit("checkpoint_recovery_failed", level="error", error=str(error))

    def save(self):
        temporary = os.path.join(
            self.directory,
            f".state.json.{os.getpid()}.{time.monotonic_ns()}.tmp",
        )
        payload = json.dumps(self.state, separators=(",", ":"), sort_keys=True)
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            directory = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            os.chmod(self.path, 0o600)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def record_gap(self, stream, reason, **details):
        gaps = self.state[stream].setdefault("gaps", [])
        gaps.append(
            {
                "at_unix": int(time.time()),
                "reason": reason,
                **details,
            }
        )
        del gaps[:-MAX_GAPS]
        emit("checkpoint_gap", level="warning", stream=stream, reason=reason, **details)

    def advance(self, stream, cursor, **updates):
        state = self.state[stream]
        state["cursor"] = cursor
        state["initialized"] = True
        state["last_success_unix"] = int(time.time())
        state.update(updates)

    def baselines(self):
        value = self.state["metrics"].get("baselines")
        if not isinstance(value, dict):
            value = {}
            self.state["metrics"]["baselines"] = value
        return copy.deepcopy(value)
