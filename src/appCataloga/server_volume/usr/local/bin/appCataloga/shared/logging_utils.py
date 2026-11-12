"""
Shared structured logger for appCataloga services and utility scripts.

The logger is intentionally lightweight: it formats deterministic log lines,
resolves one log file per entrypoint (for example `appCataloga_discovery.log`),
trims its oldest lines in place when it grows too large, and offers a small structured-event API without
depending on Python's heavier logging configuration machinery.
"""

from __future__ import annotations

import fcntl
import os
import re
import sys
import time
from datetime import datetime
from typing import List, Optional, Union

import config as k  # config MUST be loaded by the application entrypoint


# =====================================================================
# Log
# =====================================================================
class log:
    """
    Shared structured logger for appCataloga entrypoints and helpers.

    Design goals:
        1. produce one deterministic line format across scripts
        2. keep the public API tiny (`entry`, `warning`, `error`, `event`, ...)
        3. keep each entrypoint writing to its own log file by default
        4. stay bounded for long-running daemons without backup files
        5. remain simple enough to use from utility scripts without extra setup
    """

    def __init__(
        self,
        logger_name: Optional[Union[str, bool, dict]] = None,
        verbose: Union[bool, dict] = getattr(k, "LOG_VERBOSE", False),
        target_screen: bool = getattr(k, "LOG_TARGET_SCREEN", False),
        target_file: bool = getattr(k, "LOG_TARGET_FILE", False),
        log_file_name: Optional[str] = None,
    ) -> None:
        """
        Initialize a shared logger instance.

        The first positional argument may be either:
        - a logger name (`log("appCataloga_discovery")`)
        - a legacy verbosity flag (`log(True)` / `log({...})`)
        """
        if isinstance(logger_name, str):
            resolved_logger_name = self._normalize_logger_name(logger_name)
        else:
            resolved_logger_name = self._derive_logger_name()
            if logger_name is not None:
                verbose = logger_name

        self.target_screen = target_screen
        self.target_file = target_file
        self.logger_name = resolved_logger_name
        self.log_file_name = (
            log_file_name or self._resolve_log_file_name(resolved_logger_name)
        )
        self.last_update = datetime.now()
        self.last_msg = ""
        self.error_msg: List[tuple[int, str]] = []
        self.pid = os.getpid()
        self.script_name = os.path.basename(sys.argv[0]) if sys.argv else "app"
        self.max_file_size_bytes = max(
            0,
            int(getattr(k, "LOG_MAX_FILE_SIZE_MB", 0) * 1024 * 1024),
        )

        if isinstance(verbose, dict):
            self.verbose = {
                "log": bool(verbose.get("log", False)),
                "warning": bool(verbose.get("warning", False)),
                "error": bool(verbose.get("error", False)),
            }
        elif isinstance(verbose, bool):
            self.verbose = {
                "log": verbose,
                "warning": verbose,
                "error": verbose,
            }
        else:
            self.verbose = {"log": False, "warning": False, "error": False}

        self._fh = None
        if self.target_file:
            try:
                self._reopen_file_handle()
                self._write("INFO", "logger initialized")
            except Exception as e:
                self._fh = None
                self.target_file = False
                self.warning(
                    f"Invalid log_file_name '{self.log_file_name}'. "
                    f"Disabling file logging. Error: {e}"
                )

    @staticmethod
    def _normalize_logger_name(value: str) -> str:
        """
        Convert a user-provided logger name into a filesystem-safe stem.
        """
        candidate = os.path.basename(value).strip()
        if candidate.endswith(".py"):
            candidate = candidate[:-3]
        candidate = candidate or "appCataloga"
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", candidate)

    def _derive_logger_name(self) -> str:
        """
        Derive the logger name from the current script name.
        """
        script_name = os.path.basename(sys.argv[0]) if sys.argv else "appCataloga"
        return self._normalize_logger_name(script_name)

    def _resolve_log_file_name(self, logger_name: str) -> str:
        """
        Resolve the log file path for the current logger.

        By default each entrypoint receives its own file derived from the
        script name, so `appCataloga_discovery.py` naturally lands in
        `appCataloga_discovery.log`.
        """
        if hasattr(k, "LOG_DIR"):
            return os.path.join(
                getattr(k, "LOG_DIR"),
                getattr(k, "LOG_FILE_TEMPLATE", "{logger_name}.log").format(
                    logger_name=logger_name
                ),
            )

        legacy_log_file = getattr(k, "LOG_FILE", "/tmp/appCataloga.log")
        log_dir = os.path.dirname(legacy_log_file) or "/tmp"
        return os.path.join(log_dir, f"{logger_name}.log")

    # ---------------------------- internal helpers ----------------------------
    def _reopen_file_handle(self) -> None:
        """
        Open or reopen the active log file in append mode.

        This helper is reused when the active log path was deleted or replaced,
        keeping each process attached to the current file.
        """
        log_dir = os.path.dirname(self.log_file_name) or "."
        os.makedirs(log_dir, exist_ok=True)

        if self._fh:
            try:
                self._fh.close()
            except Exception:
                pass

        self._fh = open(self.log_file_name, "a", buffering=1, encoding="utf-8")

    def _sync_file_handle(self) -> None:
        """
        Reattach the file handle if the active log path was replaced or removed.

        The normal contract is "one entrypoint -> one log file". Even so, the
        same daemon can run multiple instances or worker processes that append
        to that one per-script file. If an operator deletes or replaces it,
        each process must reopen the active path instead of writing to an
        unlinked inode.
        """
        if not self.target_file:
            return

        if self._fh is None:
            self._reopen_file_handle()
            return

        try:
            current_path_stat = os.stat(self.log_file_name)
            current_handle_stat = os.fstat(self._fh.fileno())
            if current_path_stat.st_ino != current_handle_stat.st_ino:
                self._reopen_file_handle()
        except FileNotFoundError:
            self._reopen_file_handle()
        except Exception:
            self._reopen_file_handle()

    def _trim_log_if_needed(self, incoming_bytes: int) -> None:
        """
        Drop the oldest lines when the next write would exceed the limit.

        The caller must hold the exclusive lock on the log file. The newest
        half of the limit is kept in the same file, starting at a line
        boundary, so each entrypoint never has backup generations.
        """
        if self.max_file_size_bytes <= 0:
            return

        current_size = os.fstat(self._fh.fileno()).st_size
        if current_size + incoming_bytes <= self.max_file_size_bytes:
            return

        keep_bytes = self.max_file_size_bytes // 2
        with open(self.log_file_name, "r+b") as fh:
            read_pos = max(0, current_size - keep_bytes)
            if read_pos:
                fh.seek(read_pos - 1)
                fh.readline()
                read_pos = fh.tell()

            # Copy forward in chunks; the destination always trails the source.
            write_pos = 0
            while True:
                fh.seek(read_pos)
                chunk = fh.read(1024 * 1024)
                if not chunk:
                    break
                read_pos += len(chunk)
                fh.seek(write_pos)
                fh.write(chunk)
                write_pos += len(chunk)
            fh.truncate(write_pos)

    def _write(self, level: str, msg: str) -> None:
        """
        Write one fully formatted line to the configured targets.

        The logger writes plain UTF-8 lines so log files stay easy to inspect
        with shell tools. Trimming and writing share one lock on the log file
        itself, so concurrent writers never lose lines during a trim.
        """
        self.last_update = datetime.now()
        self.last_msg = msg
        timestamp = self.last_update.strftime("%Y-%m-%d %H:%M:%S")
        line = (
            f"{timestamp} | level={level:<5} | logger={self.logger_name} "
            f"| pid={self.pid} | script={self.script_name} | {msg}\n"
        )

        if self.target_file and self._fh:
            try:
                self._sync_file_handle()
                fd = self._fh.fileno()
                fcntl.flock(fd, fcntl.LOCK_EX)
                try:
                    self._trim_log_if_needed(len(line.encode("utf-8")))
                    self._fh.write(line)
                    self._fh.flush()
                finally:
                    fcntl.flock(fd, fcntl.LOCK_UN)
            except Exception:
                self.target_file = False
                print(line, end="")

        if self.target_screen:
            print(line, end="")

    @staticmethod
    def _stringify_value(value) -> str:
        """
        Convert a log field value into a compact string representation.
        """
        if isinstance(value, bool):
            return "true" if value else "false"

        if value is None:
            return "none"

        if isinstance(value, (list, tuple, set)):
            return "[" + ",".join(str(item) for item in value) + "]"

        return str(value)

    @staticmethod
    def _normalize_phase_timings(phase_durations: dict | None) -> dict:
        """
        Convert one phase-duration mapping into `<phase>_sec` log fields.
        """
        if not isinstance(phase_durations, dict):
            return {}

        normalized = {}
        for phase_name, duration in phase_durations.items():
            if duration is None:
                continue
            key = re.sub(r"[^A-Za-z0-9_]+", "_", str(phase_name)).strip("_")
            if not key:
                continue
            normalized[f"{key}_sec"] = duration

        return normalized

    def format_event(self, event: str, **fields) -> str:
        """
        Build a structured event message in `event=... key=value` format.
        """
        parts = [f"event={event}"]

        for key, value in fields.items():
            if value is None:
                continue

            normalized_key = re.sub(r"[^A-Za-z0-9_]+", "_", str(key)).strip("_")
            if not normalized_key:
                continue

            parts.append(
                f"{normalized_key}={self._stringify_value(value)}"
            )

        return " ".join(parts)

    # ------------------------------- public API --------------------------------
    def entry(self, message: str) -> None:
        """
        Write an informational log entry when enabled by verbosity.
        """
        if self.verbose.get("log", False):
            self._write("INFO", str(message))

    def warning(self, message: str) -> None:
        """
        Write a warning log entry when enabled by verbosity.
        """
        if self.verbose.get("warning", False):
            self._write("WARN", str(message))

    def error(self, message: str) -> None:
        """
        Write an error log entry.

        Error messages are always accumulated in memory, even when not printed.
        """
        self.error_msg.append((int(time.time()), str(message)))
        if self.verbose.get("error", True):
            self._write("ERROR", str(message))

    def event(self, event: str, **fields) -> None:
        """
        Write an informational structured event.
        """
        self.entry(self.format_event(event, **fields))

    def warning_event(self, event: str, **fields) -> None:
        """
        Write a warning structured event.
        """
        self.warning(self.format_event(event, **fields))

    def error_event(self, event: str, **fields) -> None:
        """
        Write an error structured event.
        """
        self.error(self.format_event(event, **fields))

    def service_start(self, service: str, **fields) -> None:
        """
        Write a standard service start event.
        """
        self.event("service_start", service=service, **fields)

    def service_stop(self, service: str, **fields) -> None:
        """
        Write a standard service stop event.
        """
        self.event("service_stop", service=service, **fields)

    def task_claimed(
        self,
        service: str,
        *,
        host_id=None,
        task_id=None,
        task_type=None,
        worker_id=None,
        file=None,
        **fields,
    ) -> None:
        """
        Write the canonical event for one claimed queue row.
        """
        self.event(
            "task_claimed",
            service=service,
            host_id=host_id,
            task_id=task_id,
            task_type=task_type,
            worker_id=worker_id,
            file=file,
            **fields,
        )

    def task_done(
        self,
        service: str,
        *,
        host_id=None,
        task_id=None,
        task_type=None,
        worker_id=None,
        file=None,
        elapsed_sec=None,
        **fields,
    ) -> None:
        """
        Write the canonical event for one successfully completed queue row.
        """
        self.event(
            "task_done",
            service=service,
            host_id=host_id,
            task_id=task_id,
            task_type=task_type,
            worker_id=worker_id,
            file=file,
            elapsed_sec=elapsed_sec,
            **fields,
        )

    def task_phase(
        self,
        service: str,
        *,
        host_id=None,
        task_id=None,
        task_type=None,
        worker_id=None,
        file=None,
        phase=None,
        elapsed_sec=None,
        since_start_sec=None,
        **fields,
    ) -> None:
        """
        Write the canonical intermediate progress event for one task phase.
        """
        self.event(
            "task_phase",
            service=service,
            host_id=host_id,
            task_id=task_id,
            task_type=task_type,
            worker_id=worker_id,
            file=file,
            phase=phase,
            elapsed_sec=elapsed_sec,
            since_start_sec=since_start_sec,
            **fields,
        )

    def task_error(
        self,
        service: str,
        *,
        host_id=None,
        task_id=None,
        task_type=None,
        worker_id=None,
        file=None,
        stage=None,
        error=None,
        **fields,
    ) -> None:
        """
        Write the canonical event for one terminal task failure.
        """
        self.error_event(
            "task_error",
            service=service,
            host_id=host_id,
            task_id=task_id,
            task_type=task_type,
            worker_id=worker_id,
            file=file,
            stage=stage,
            error=error,
            **fields,
        )

    def task_frozen(
        self,
        service: str,
        *,
        host_id=None,
        task_id=None,
        task_type=None,
        worker_id=None,
        file=None,
        stage=None,
        error=None,
        **fields,
    ) -> None:
        """
        Write the canonical event for one task frozen for manual review.
        """
        self.warning_event(
            "task_frozen",
            service=service,
            host_id=host_id,
            task_id=task_id,
            task_type=task_type,
            worker_id=worker_id,
            file=file,
            stage=stage,
            error=error,
            **fields,
        )

    def task_phase_timings(
        self,
        service: str,
        *,
        host_id=None,
        task_id=None,
        task_type=None,
        worker_id=None,
        file=None,
        outcome: str,
        total_sec: float,
        phase_durations: dict | None = None,
        **fields,
    ) -> None:
        """
        Write the canonical timing event for one task iteration.
        """
        self.event(
            "task_phase_timings",
            service=service,
            host_id=host_id,
            task_id=task_id,
            task_type=task_type,
            worker_id=worker_id,
            file=file,
            outcome=outcome,
            total_sec=round(total_sec, 3),
            **self._normalize_phase_timings(phase_durations),
            **fields,
        )

    def signal_received(self, signal_name: str, **fields) -> None:
        """
        Write a standard signal reception event.
        """
        self.event("signal_received", signal=signal_name, **fields)

    def dump_error(self) -> str:
        """Return all collected error messages as a single string."""
        return ", ".join([m for _, m in self.error_msg])

    def close(self) -> None:
        """
        Close the current file handle explicitly.

        Long-running daemons typically keep the logger alive for the whole
        process lifetime, but tests and short utilities benefit from an
        explicit shutdown hook.
        """
        try:
            if self._fh:
                self._fh.close()
        finally:
            self._fh = None

    def __del__(self) -> None:
        """Close the log file handle upon garbage collection (best-effort)."""
        try:
            self.close()
        except Exception:
            pass
