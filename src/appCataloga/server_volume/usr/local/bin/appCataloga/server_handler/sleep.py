"""
Small sleep helpers shared by long-running workers.

This module owns the tiny backoff primitives that used to live in
``shared.legacy`` so polling loops can use them without importing a generic
compatibility bucket.

These helpers are intentionally small. Their job is not to implement retry
policy; they only provide the low-level timing primitives that workers reuse.
"""

from __future__ import annotations

import os
import random
import sys
import time

# ---------------------------------------------------------------------
# This module may be imported by standalone worker scripts executed directly
# from the filesystem. We therefore keep the historical config bootstrap here
# so the helper remains drop-in compatible in those runtime shapes.
# ---------------------------------------------------------------------
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402  (must be available at runtime)


def random_jitter_sleep() -> None:
    """
    Sleep a small randomized interval to reduce worker polling races.

    Workers use this between idle polls so many daemons do not wake and
    contend for the same rows in perfect lockstep.
    """
    time.sleep(random.uniform(0.5, k.MAX_HOST_TASK_WAIT_TIME))
