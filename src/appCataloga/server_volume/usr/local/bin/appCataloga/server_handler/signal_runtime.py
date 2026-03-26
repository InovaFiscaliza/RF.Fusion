"""
Shared shutdown-signal helpers for appCataloga entrypoints.

Most daemons react to SIGTERM/SIGINT the same way: mark the loop as no longer
running, emit a structured log line, and optionally execute a small cleanup
callback. Centralizing that wiring removes repetitive wrapper functions from
each worker while still allowing specialized shutdown logic when needed.

The entrypoint remains the owner of shutdown policy. This module only
standardizes the low-level signal hook so each worker does not need to repeat
the same `signal.signal(...)` boilerplate.
"""

from __future__ import annotations

import signal
from typing import Any, Callable


def install_shutdown_handlers(
    *,
    process_status: dict,
    logger: Any,
    on_shutdown: Callable[[str], None] | None = None,
    log_fields: dict | None = None,
) -> None:
    """
    Register SIGTERM/SIGINT handlers for a long-running appCataloga process.

    The installed handlers do three things in a fixed order:
        1. flip `process_status["running"]` to stop the outer loop
        2. emit one structured shutdown log line
        3. run optional caller-specific cleanup, such as waking a selector
    """
    extra_fields = dict(log_fields or {})

    def _build_handler(signal_name: str, handler_name: str):
        def _handler(signum=None, frame=None) -> None:
            # The shared contract is "request shutdown, log once, then let the
            # entrypoint unwind normally". Heavy cleanup stays outside the
            # signal handler and is injected by the caller when needed.
            process_status["running"] = False
            logger.signal_received(
                signal_name,
                handler=handler_name,
                **extra_fields,
            )
            if on_shutdown is not None:
                on_shutdown(signal_name)

        return _handler

    # Installing both handlers in one place keeps long-running workers
    # consistent and makes shutdown behavior predictable across scripts.
    signal.signal(
        signal.SIGTERM,
        _build_handler("SIGTERM", "sigterm_handler"),
    )
    signal.signal(
        signal.SIGINT,
        _build_handler("SIGINT", "sigint_handler"),
    )
