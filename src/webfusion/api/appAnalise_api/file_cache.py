"""Bound file-search reuse and coordinate concurrent database builds."""

from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import dataclass
import sys
from threading import BoundedSemaphore, Lock
import time
from typing import Callable

from . import config as k
from .filters import Filters


FileValue = tuple[int, ...] | int
FileKey = tuple[str, Filters]


@dataclass
class _Entry:
    """Retain one immutable search value with a fixed lifetime.

    Attributes:
        value: Ordered file IDs (tuple[int, ...]) or independent count (int).
        expires_at: Expiry in monotonic seconds (float).
        size_bytes: Accounted Python value size, including tuple items (int).
    """

    value: FileValue
    expires_at: float
    size_bytes: int


class FileQueryCache:
    """Reuse immutable file searches without sharing database resources.

    Attributes:
        _entries: LRU entries keyed by operation and normalized filters;
            initially empty (OrderedDict[FileKey, _Entry]).
        _pending: Builds shared by requests for the same key; initially empty
            (dict[FileKey, Future[FileValue]]).
        _bytes: Accounted size of retained values; initially zero (int).
        _lock: Mutex protecting cache and pending state (Lock).
        _build_slots: Limit for concurrent database builds (BoundedSemaphore).
    """

    def __init__(self) -> None:
        """Initialize empty process-local state.

        Args:
            None.

        Returns:
            None.
        """
        self._entries: OrderedDict[FileKey, _Entry] = OrderedDict()
        self._pending: dict[FileKey, Future[FileValue]] = {}
        self._bytes = 0
        self._lock = Lock()
        self._build_slots = BoundedSemaphore(k.FILE_QUERY_BUILD_CONCURRENCY)

    def _expire(self, now: float) -> None:
        """Remove expired entries while the caller holds the mutex.

        Args:
            now: Current monotonic time in seconds (float).

        Returns:
            None. Updates entries and byte accounting.
        """
        for key in [key for key, entry in self._entries.items() if entry.expires_at <= now]:
            self._bytes -= self._entries.pop(key).size_bytes

    def get_or_load(self, key: FileKey, load: Callable[[], FileValue]) -> FileValue:
        """Return a fresh value or share one bounded build with concurrent callers.

        Args:
            key: Operation and normalized filters (tuple[str, Filters]).
            load: Database loader returning immutable IDs or count; called
                outside the mutex (Callable[[], tuple[int, ...] | int]).

        Returns:
            Complete ordered IDs or count (tuple[int, ...] | int). Oversized
            values are returned to all waiters without retaining or truncating.

        Raises:
            BaseException: Loader failures propagate to all current waiters;
                subsequent requests may retry. No failed result is cached.
        """
        with self._lock:
            self._expire(time.monotonic())
            if key in self._entries:
                self._entries.move_to_end(key)
                return self._entries[key].value
            future = self._pending.get(key)
            if future is None:
                owner = True
                future = Future()
                self._pending[key] = future
            else:
                owner = False

        if not owner:
            return future.result()

        try:
            with self._build_slots:
                value = load()
            size_bytes = sys.getsizeof(value)
            if isinstance(value, tuple):
                size_bytes += sum(sys.getsizeof(item) for item in value)
            with self._lock:
                now = time.monotonic()
                self._expire(now)
                if size_bytes <= k.FILE_QUERY_CACHE_MAX_BYTES:
                    while self._entries and (
                        len(self._entries) >= k.FILE_QUERY_CACHE_MAX_ENTRIES
                        or self._bytes + size_bytes > k.FILE_QUERY_CACHE_MAX_BYTES
                    ):
                        _, removed = self._entries.popitem(last=False)
                        self._bytes -= removed.size_bytes
                    self._entries[key] = _Entry(
                        value, now + k.FILE_QUERY_CACHE_TTL_SECONDS, size_bytes,
                    )
                    self._bytes += size_bytes
                self._pending.pop(key)
                future.set_result(value)
            return value
        except BaseException as exc:
            # Every waiter must be released even if a build is interrupted.
            with self._lock:
                self._pending.pop(key, None)
                future.set_exception(exc)
            raise
