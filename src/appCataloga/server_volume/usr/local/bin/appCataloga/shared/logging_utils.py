"""
Shared structured logger for appCataloga services and utility scripts.

The logger is intentionally lightweight: it formats deterministic log lines,
resolves one log file per entrypoint (for example `appCataloga_discovery.log`),
rotates oversized files, and offers a small structured-event API without
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
        4. stay safe for long-running daemons by rotating oversized files
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
        self.max_backup_files = max(
            0,
            int(getattr(k, "LOG_MAX_BACKUP_FILES", 0)),
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

        This helper is reused after external rotation as well as after this
        logger rotates its own file. Reopening keeps each process attached to
        the current active path instead of writing forever to a renamed inode.
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
        Reattach the file handle if another process already rotated this log.

        The normal contract is "one entrypoint -> one log file". Even so, the
        same daemon can run multiple instances or worker processes that append
        to that one per-script file. If one instance rotates it, the others
        must reopen the active path or they would keep writing to the renamed
        backup file.
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

    def _rotate_logs_if_needed(self, incoming_bytes: int) -> None:
        """
        Rotate the active log file when the next write would exceed the limit.

        Rotation is best-effort but process-safe:
            - rotation happens independently for each log file / script
            - an exclusive lock serializes concurrent rotations
            - backups shift to `.1`, `.2`, ... up to the configured retention
            - the oldest generation is deleted first when retention is exceeded
        """
        if (
            not self.target_file
            or self.max_file_size_bytes <= 0
            or self.max_backup_files <= 0
        ):
            return

        lock_path = f"{self.log_file_name}.lock"
        os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)

        with open(lock_path, "a", encoding="utf-8") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)

            self._sync_file_handle()

            try:
                current_size = os.path.getsize(self.log_file_name)
            except FileNotFoundError:
                current_size = 0

            if current_size + incoming_bytes <= self.max_file_size_bytes:
                return

            if self._fh:
                try:
                    self._fh.flush()
                except Exception:
                    pass

            oldest_backup = f"{self.log_file_name}.{self.max_backup_files}"
            if os.path.exists(oldest_backup):
                os.remove(oldest_backup)

            for index in range(self.max_backup_files - 1, 0, -1):
                src = f"{self.log_file_name}.{index}"
                dst = f"{self.log_file_name}.{index + 1}"
                if os.path.exists(src):
                    os.replace(src, dst)

            if os.path.exists(self.log_file_name):
                os.replace(self.log_file_name, f"{self.log_file_name}.1")

            self._reopen_file_handle()

    def _write(self, level: str, msg: str) -> None:
        """
        Write one fully formatted line to the configured targets.

        The logger writes plain UTF-8 lines so log files stay easy to inspect
        with shell tools. File rotation happens immediately before the write so
        recurring daemons cannot grow logs without bound.
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
                self._rotate_logs_if_needed(len(line.encode("utf-8")))
                self._sync_file_handle()
                self._fh.write(line)
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
