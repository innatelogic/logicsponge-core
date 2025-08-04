"""Executor backend."""

from collections.abc import Callable, Iterable
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any


class ExecutorBackend:
    """The backend that can execute tasks."""

    def __init__(self, max_workers: int | None = None) -> None:
        """Create a backend."""
        self._executor = ProcessPoolExecutor(max_workers=max_workers)

    def submit(self, fn: Callable[..., Any], *args, **kwargs) -> Future:
        """Submit a task."""
        return self._executor.submit(fn, *args, **kwargs)

    def map(self, fn: Callable[[Any], Any], iterable: Iterable[Any]) -> Iterable[Any]:
        """Run the standard map."""
        return self._executor.map(fn, iterable)

    def shutdown(self, *args, wait: bool = True) -> None:
        """Shutdown the executor."""
        self._executor.shutdown(wait=wait)
