"""
Small process-control helpers for appCataloga entrypoints.

These helpers keep per-service entrypoints focused on orchestration instead of
embedding repeated shutdown and sibling-process control code inline.

Reading guide:
    - `wake_selector(...)` is the tiny primitive used by signal handlers
    - `stop_self_service(...)` is the heavier best-effort sibling teardown
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shared.logging_utils import log as logger_type


def wake_selector(write_fd: int) -> None:
    """
    Wake a selector loop by writing a byte to its control pipe.

    This helper is intentionally tiny because it is often called from signal
    handling paths, where the safest behavior is "best effort, never raise".
    """
    try:
        os.write(write_fd, b"\0")
    except Exception:
        pass


def stop_self_service(
    *,
    script_name: str,
    logger: logger_type,
    grace_seconds: float = 2.0,
) -> None:
    """
    Stop sibling processes whose command line matches ``script_name``.

    The current process is never signaled by this helper.

    Flow:
        1. discover candidate PIDs with `pgrep -f`
        2. send SIGTERM to sibling matches
        3. wait a short grace period
        4. escalate stubborn siblings to SIGKILL

    Entry points call this only during fatal failure or final teardown. By the
    time we reach this helper, recovery matters more than perfect process
    bookkeeping, so the implementation is intentionally defensive and
    log-oriented.
    """
    try:
        result = subprocess.run(
            ["pgrep", "-f", script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        pids = [
            int(pid)
            for pid in result.stdout.split()
            if pid.strip().isdigit()
        ]
        current_pid = os.getpid()

        if not pids:
            logger.event(
                "service_stop_scan_empty",
                component="process_control",
                operation="stop_self_service",
                script_name=script_name,
            )
            return

        # Phase 1: ask sibling processes to stop cleanly first.
        for pid in pids:
            if pid == current_pid:
                continue

            try:
                os.kill(pid, signal.SIGTERM)
                logger.event(
                    "service_stop_signal_sent",
                    component="process_control",
                    operation="stop_self_service",
                    signal="SIGTERM",
                    pid=pid,
                )
            except ProcessLookupError:
                continue
            except Exception as exc:
                logger.warning_event(
                    "service_stop_signal_failed",
                    component="process_control",
                    operation="stop_self_service",
                    signal="SIGTERM",
                    pid=pid,
                    error=exc,
                )

        # Give cooperative siblings a short window to flush logs and exit
        # before hard escalation.
        time.sleep(grace_seconds)

        # Phase 2: any sibling still alive after the grace window is treated
        # as orphaned/stuck and escalated to SIGKILL.
        for pid in pids:
            if pid == current_pid:
                continue

            try:
                os.kill(pid, 0)
            except OSError:
                continue

            os.kill(pid, signal.SIGKILL)
            logger.warning_event(
                "service_stop_signal_sent",
                component="process_control",
                operation="stop_self_service",
                signal="SIGKILL",
                pid=pid,
            )

    except Exception as exc:
        # Teardown helpers must fail closed. We log the problem but never let
        # process-control cleanup raise back into the caller.
        logger.error_event(
            "service_stop_failed",
            component="process_control",
            operation="stop_self_service",
            script_name=script_name,
            error=exc,
        )
