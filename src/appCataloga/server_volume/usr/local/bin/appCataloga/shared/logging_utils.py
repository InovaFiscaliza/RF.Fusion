from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from typing import List, Union

import config as k  # config MUST be loaded by the application entrypoint


# =====================================================================
# Log
# =====================================================================
class log:
    """Simple logger with optional stdout and file outputs.

    This implementation keeps a file handle open to reduce I/O overhead,
    but preserves the original public methods and behavior.

    Attributes:
        target_screen (bool): If True, prints log entries to stdout.
        target_file (bool): If True, appends log entries to a file.
        log_file_name (str): Target log file path when target_file=True.
        last_update (datetime): Timestamp of last log entry.
        last_msg (str): Last message written.
        error_msg (List[Tuple[int, str]]): Collected error messages (epoch, msg).
        pid (int): Current process ID.
        pname (str): Current process name (argv[0]).
        verbose (dict): Verbosity flags for 'log', 'warning', 'error'.
    """

    def __init__(
        self,
        verbose: Union[bool, dict] = getattr(k, "LOG_VERBOSE", False),
        target_screen: bool = getattr(k, "LOG_TARGET_SCREEN", False),
        target_file: bool = getattr(k, "LOG_TARGET_FILE", False),
        log_file_name: str = getattr(k, "LOG_FILE", "/tmp/appCataloga.log"),
    ) -> None:
        """Initialize logger.
        """
        self.target_screen = target_screen
        self.target_file = target_file
        self.log_file_name = log_file_name
        self.last_update = datetime.now()
        self.last_msg = ""
        self.error_msg: List[tuple[int, str]] = []
        self.pid = os.getpid()
        self.pname = os.path.basename(sys.argv[0]) if sys.argv else "app"

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
                self._fh = open(self.log_file_name, "a", buffering=1, encoding="utf-8")
                self._write("Log started")
            except Exception as e:
                self._fh = None
                self.target_file = False
                self.warning(
                    f"Invalid log_file_name '{self.log_file_name}'. "
                    f"Disabling file logging. Error: {e}"
                )

    # ---------------------------- internal helpers ----------------------------
    def _write(self, msg: str) -> None:
        """Write a formatted message to the configured targets."""
        self.last_update = datetime.now()
        self.last_msg = msg
        timestamp = self.last_update.strftime("%Y/%m/%d %H:%M:%S")
        line = f"{timestamp} | p.{self.pid} | {self.pname} | {msg}\n"

        if self.target_file and self._fh:
            try:
                self._fh.write(line)
            except Exception:
                self.target_file = False
                print(line, end="")

        if self.target_screen:
            print(line, end="")

    # ------------------------------- public API --------------------------------
    def entry(self, message: str) -> None:
        """Write a standard (info) log entry when enabled by verbosity."""
        if self.verbose.get("log", False):
            self._write(str(message))

    def warning(self, message: str) -> None:
        """Write a warning log entry when enabled by verbosity."""
        if self.verbose.get("warning", False):
            self._write(f"[WARN] {message}")

    def error(self, message: str) -> None:
        """Write an error log entry (always stored in memory; printed if enabled)."""
        self.error_msg.append((int(time.time()), str(message)))
        if self.verbose.get("error", True):
            self._write(f"[ERROR] {message}")

    def dump_error(self) -> str:
        """Return all collected error messages as a single string."""
        return ", ".join([m for _, m in self.error_msg])

    def __del__(self) -> None:
        """Close the log file handle upon garbage collection (best-effort)."""
        try:
            if self._fh:
                self._fh.close()
        except Exception:
            pass
