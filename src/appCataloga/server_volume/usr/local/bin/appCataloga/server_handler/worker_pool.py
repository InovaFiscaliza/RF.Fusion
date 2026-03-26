"""
Process-based worker-pool helpers shared by appCataloga daemons.

The backup worker uses a detached multi-process pool. Keeping the lifecycle
helpers here lets the worker file focus on the file pipeline instead of mixing
process-management utilities ahead of `main()`.

Reading guide:
    1. discovery helpers
       Find currently running workers and recover worker IDs from command
       lines.
    2. growth helpers
       Spawn additional workers when demand justifies scale-out.
    3. recovery and retirement helpers
       Repair a broken pool shape and decide when idle extra workers may exit.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from typing import Any


def extract_worker_id_from_cmdline(args: list, process_filename: str):
    """
    Resolve the worker ID represented by a process command line.

    The seed process is commonly started without an explicit `worker=0`
    argument by the service wrapper or an IDE debug session. In that case,
    once we confirm the command line is running this script, we must still
    treat it as worker 0; otherwise the pool manager never sees the seed
    and on-demand scale-out stalls after the first task.
    """
    for arg in args:
        if arg.startswith("worker="):
            try:
                return int(arg.split("=")[1])
            except ValueError:
                return None

    normalized_args = [
        os.path.basename(arg)
        for arg in args
        if isinstance(arg, str) and arg
    ]

    if process_filename in normalized_args:
        return 0

    return None


def list_running_worker_processes(process_filename: str) -> list[tuple[int, int]]:
    """
    Detect detached worker processes with their PID and worker ID.

    This is the lowest-level pool scan helper. Richer helpers built on top of
    it usually convert the result into just worker IDs or act on the pool
    shape.
    """
    processes = []
    try:
        pids = os.popen(f"pgrep -f {process_filename}").read().splitlines()
        for pid_text in pids:
            cmdline = f"/proc/{pid_text}/cmdline"
            if not os.path.exists(cmdline):
                continue

            args = open(cmdline).read().split("\x00")
            worker_id = extract_worker_id_from_cmdline(args, process_filename)
            if worker_id is None:
                continue

            try:
                pid = int(pid_text)
            except ValueError:
                continue

            processes.append((pid, worker_id))
    except Exception:
        # Pool scans are advisory. If discovery fails, the caller should keep
        # running conservatively instead of crashing the worker.
        pass

    return sorted(set(processes), key=lambda item: (item[1], item[0]))


def list_running_workers(process_filename: str, *, logger: Any) -> list[int]:
    """
    Return the currently running worker IDs for a worker script.

    The log line is part of the contract here: pool-management decisions are
    much easier to audit when every scan reports the active shape it saw.
    """
    workers = sorted(
        {worker_id for _, worker_id in list_running_worker_processes(process_filename)}
    )
    logger.event("worker_pool_scan", active_workers=workers)
    return workers


def broadcast_shutdown_to_worker_pool(
    signal_name: str,
    *,
    process_status: dict,
    logger: Any,
    script_path: str,
) -> None:
    """
    Propagate shutdown to detached sibling workers.

    Detached workers do not automatically share the entrypoint's process
    lifetime, so the pool needs an explicit broadcast step when one worker
    decides the whole service should unwind.
    """
    if process_status.get("shutdown_broadcast_sent"):
        return

    process_status["shutdown_broadcast_sent"] = True
    current_pid = os.getpid()
    script_name = os.path.basename(script_path)
    targets = [
        (pid, worker_id)
        for pid, worker_id in list_running_worker_processes(script_name)
        if pid != current_pid
    ]

    if not targets:
        return

    logger.warning(
        f"event=worker_pool_shutdown_broadcast signal={signal_name} "
        f"sender_pid={current_pid} targets={targets}"
    )

    # The broadcast is intentionally one-way and best effort. Workers may have
    # exited between scan and signal send, so lookup races are expected.
    for pid, worker_id in targets:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        except Exception as e:
            logger.warning(
                f"event=worker_pool_shutdown_broadcast_failed "
                f"target_pid={pid} worker_id={worker_id} error={e}"
            )


def spawn_additional_worker(
    current_workers: list,
    *,
    script_path: str,
    max_workers: int,
    logger: Any,
) -> None:
    """
    Spawn the next detached worker if the pool still has capacity.

    The next worker ID is the first gap in the currently running set, not just
    `len(current_workers)`. That keeps worker numbering stable after isolated
    worker exits.
    """
    next_worker = 0
    # Reuse the first free worker ID instead of only appending at the end.
    # This keeps the pool shape compact and predictable after isolated exits.
    while next_worker in current_workers:
        next_worker += 1

    # Capacity guard: once the configured pool ceiling is reached, scale-out
    # stops immediately and the caller simply keeps working with the current
    # pool.
    if len(current_workers) >= max_workers:
        return

    try:
        subprocess.Popen(
            [sys.executable, os.path.abspath(script_path), f"worker={next_worker}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        logger.event("worker_spawned", worker_id=next_worker)
    except Exception as e:
        logger.error(f"event=worker_spawn_failed worker_id={next_worker} error={e}")


def spawn_specific_worker(worker_id: int, *, script_path: str, logger: Any) -> bool:
    """
    Spawn a specific worker ID when a gap in the pool must be repaired.

    This is used by recovery logic, where the worker ID matters because the
    pool shape itself is part of the health check.
    """
    try:
        subprocess.Popen(
            [sys.executable, os.path.abspath(script_path), f"worker={worker_id}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        logger.event("worker_spawned", worker_id=worker_id, reason="pool_recovery")
        return True
    except Exception as e:
        logger.error(
            f"event=worker_spawn_failed worker_id={worker_id} "
            f"reason=pool_recovery error={e}"
        )
        return False


def maybe_spawn_next_worker(
    worker_id: int,
    *,
    script_path: str,
    max_workers: int,
    logger: Any,
) -> None:
    """
    Expand the pool only when the current highest worker is already busy.

    This keeps pool growth conservative: only the current frontier worker is
    allowed to trigger scale-out, which avoids many workers racing to spawn
    the same successor.
    """
    try:
        script_name = os.path.basename(script_path)
        current_workers = list_running_workers(script_name, logger=logger)

        # Never scale beyond the configured ceiling.
        if len(current_workers) >= max_workers:
            return

        # If the scan returned nothing, pool discovery is too uncertain to make
        # a safe scale-out decision.
        if not current_workers:
            return

        # Only the current frontier worker may ask for a new sibling. This
        # avoids many active workers racing to create the same "next" worker.
        if worker_id != max(current_workers):
            return

        spawn_additional_worker(
            current_workers,
            script_path=script_path,
            max_workers=max_workers,
            logger=logger,
        )

    except Exception as e:
        logger.warning(
            f"event=worker_pool_scale_out_failed worker_id={worker_id} error={e}"
        )


def ensure_seed_worker_alive(
    worker_id: int,
    *,
    process_status: dict,
    script_path: str,
    max_workers: int,
    retry_seconds: float,
    logger: Any,
) -> bool:
    """
    Ensure worker 0 exists so the on-demand pool can recover itself.

    Worker 0 is the seed of this detached pool model. If it disappears while
    higher workers are still alive, scale-out and retirement logic become
    skewed, so this guard gives the pool a way to heal itself.
    """
    try:
        current_workers = list_running_workers(
            os.path.basename(script_path),
            logger=logger,
        )
        now = time.time()

        # Healthy pool shape: worker 0 is present, so clear any previous
        # recovery backoff and leave immediately.
        if 0 in current_workers:
            process_status["seed_recovery_last_attempt"] = 0.0
            return True

        # If the pool scan is empty, there is nothing trustworthy to repair
        # from this worker. Let the outer runtime decide what to do next.
        if not current_workers:
            return False

        # Do not repair the seed by exceeding the configured pool size.
        if len(current_workers) >= max_workers:
            return False

        # Only the lowest surviving worker attempts the repair. This elects one
        # temporary leader and avoids duplicate respawn attempts.
        if worker_id != min(current_workers):
            return False

        last_attempt = process_status.get("seed_recovery_last_attempt", 0.0)
        # Back off repeated repair attempts so a broken environment does not
        # spin in a tight respawn loop.
        if now - last_attempt < retry_seconds:
            return False

        # Only one worker should attempt the repair, and only after the retry
        # window opens, otherwise the pool can thrash on repeated spawn tries.
        process_status["seed_recovery_last_attempt"] = now
        logger.warning(
            f"event=worker_seed_missing worker_id={worker_id} "
            f"active_workers={current_workers}"
        )
        return spawn_specific_worker(0, script_path=script_path, logger=logger)

    except Exception as e:
        logger.warning(
            f"event=worker_seed_guard_failed worker_id={worker_id} error={e}"
        )
        return False


def should_retire_idle_worker(
    worker_id: int,
    idle_cycles: int,
    *,
    script_path: str,
    idle_exit_cycles: int,
    logger: Any,
) -> bool:
    """
    Decide whether an extra worker should exit after repeated idle polls.

    The pool always keeps worker 0 alive. Extra workers become eligible for
    retirement only after enough consecutive idle cycles and only when the
    seed worker is still present.
    """
    # Worker 0 is the anchor of this detached pool model and is never retired
    # by idle logic.
    if worker_id == 0:
        return False

    # A worker must stay idle for long enough before it is considered
    # disposable; short quiet periods are normal and should not shrink the
    # pool immediately.
    if idle_cycles < idle_exit_cycles:
        return False

    current_workers = list_running_workers(
        os.path.basename(script_path),
        logger=logger,
    )

    # If the seed is missing, pool shape is already degraded. In that state we
    # prefer repair logic over retirement logic.
    if 0 not in current_workers:
        return False

    # Retire only when there is still another worker alive after this one
    # exits, otherwise the pool could collapse entirely.
    return len(current_workers) > 1
