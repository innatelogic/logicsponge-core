"""Thread pool management for logicsponge-core."""

import concurrent.futures
import logging
import os
import threading
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class LogicSpongeThreadPool:
    """Global thread pool for logicsponge terms."""

    _instance: Optional["LogicSpongeThreadPool"] = None
    _lock = threading.Lock()

    def __new__(cls, max_workers: Optional[int] = None):
        """Create or return singleton thread pool instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, max_workers: Optional[int] = None):
        """Initialize thread pool (only once due to singleton)."""
        if self._initialized:
            return

        if max_workers is None:
            # Default: CPU count * 2, capped at reasonable limit
            max_workers = min(32, (os.cpu_count() or 1) * 2)

        self.max_workers = max_workers
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="logicsponge")
        self.active_tasks = 0
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.task_lock = threading.Lock()
        self._initialized = True

        logger.info(f"LogicSponge thread pool initialized with {max_workers} workers")

    def submit_term(self, term_func: Callable, stop_event: threading.Event, term_name: str = "unknown"):
        """Submit a term's execution to the thread pool."""
        with self.task_lock:
            self.active_tasks += 1

        def wrapper():
            try:
                logger.debug(f"Starting term execution: {term_name}")
                result = term_func(stop_event)
                with self.task_lock:
                    self.completed_tasks += 1
                logger.debug(f"Completed term execution: {term_name}")
                return result
            except Exception as e:
                with self.task_lock:
                    self.failed_tasks += 1
                logger.error(f"Term execution failed: {term_name} - {e}")
                raise
            finally:
                with self.task_lock:
                    self.active_tasks -= 1

        return self.executor.submit(wrapper)

    def get_utilization(self) -> float:
        """Get current thread pool utilization (0.0 to 1.0+)."""
        with self.task_lock:
            return self.active_tasks / self.max_workers if self.max_workers > 0 else 0.0

    def get_stats(self) -> dict[str, Any]:
        """Get thread pool statistics."""
        with self.task_lock:
            return {
                "max_workers": self.max_workers,
                "active_tasks": self.active_tasks,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
                "utilization": self.get_utilization(),
            }

    def shutdown(self, wait: bool = True):
        """Shutdown the thread pool."""
        logger.info("Shutting down LogicSponge thread pool")
        self.executor.shutdown(wait=wait)
        with LogicSpongeThreadPool._lock:
            LogicSpongeThreadPool._instance = None


class WorkStealingThreadPool:
    """Work-stealing thread pool for CPU-intensive workloads."""

    def __init__(self, num_workers: Optional[int] = None):
        """Initialize work-stealing thread pool."""
        if num_workers is None:
            num_workers = min(16, os.cpu_count() or 1)

        self.num_workers = num_workers
        self.work_queues = [concurrent.futures._base.Queue() for _ in range(num_workers)]
        self.workers = []
        self.running = False
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.task_lock = threading.Lock()

        logger.info(f"Work-stealing thread pool initialized with {num_workers} workers")

    def start(self):
        """Start worker threads."""
        if self.running:
            return

        self.running = True
        for i in range(self.num_workers):
            worker = threading.Thread(target=self._worker_loop, args=(i,), name=f"logicsponge-ws-{i}", daemon=False)
            worker.start()
            self.workers.append(worker)

        logger.debug(f"Started {self.num_workers} work-stealing workers")

    def _worker_loop(self, worker_id: int):
        """Main worker loop with work stealing."""
        my_queue = self.work_queues[worker_id]

        while self.running:
            task_found = False

            # Try own queue first
            try:
                task = my_queue.get_nowait()
                self._execute_task(task, worker_id)
                task_found = True
                continue
            except concurrent.futures._base.Empty:
                pass

            # Steal from other queues
            for i, queue in enumerate(self.work_queues):
                if i != worker_id:
                    try:
                        task = queue.get_nowait()
                        self._execute_task(task, worker_id)
                        task_found = True
                        break
                    except concurrent.futures._base.Empty:
                        continue

            if not task_found:
                time.sleep(0.001)  # Brief sleep if no work found

    def _execute_task(self, task, worker_id: int):
        """Execute a task and track completion."""
        try:
            task()
            with self.task_lock:
                self.completed_tasks += 1
        except Exception as e:
            with self.task_lock:
                self.failed_tasks += 1
            logger.error(f"Task failed on worker {worker_id}: {e}")

    def submit(self, task_func: Callable, *args, **kwargs):
        """Submit task to least loaded queue."""

        def task():
            return task_func(*args, **kwargs)

        # Find queue with minimum size
        min_queue = min(self.work_queues, key=lambda q: q.qsize())
        min_queue.put(task)

    def shutdown(self, timeout: float = 5.0):
        """Shutdown the work-stealing pool."""
        logger.info("Shutting down work-stealing thread pool")
        self.running = False

        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=timeout)

        # Clear queues
        for queue in self.work_queues:
            while not queue.empty():
                try:
                    queue.get_nowait()
                except concurrent.futures._base.Empty:
                    break

    def get_stats(self) -> dict[str, Any]:
        """Get work-stealing pool statistics."""
        with self.task_lock:
            queue_sizes = [q.qsize() for q in self.work_queues]
            return {
                "num_workers": self.num_workers,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
                "queue_sizes": queue_sizes,
                "total_queued": sum(queue_sizes),
                "running": self.running,
            }


# Global configuration
_thread_pool_config = {
    "enabled": True,
    "max_workers": None,
    "pool_type": "thread_pool",  # "thread_pool" or "work_stealing"
}


def configure_thread_pool(enabled: bool = True, max_workers: Optional[int] = None, pool_type: str = "thread_pool"):
    """Configure global thread pool settings for logicsponge.

    Args:
        enabled: Whether to use thread pool (True) or dedicated threads (False)
        max_workers: Maximum number of worker threads (None for auto-detect)
        pool_type: Type of pool ("thread_pool" or "work_stealing")
    """
    global _thread_pool_config

    if pool_type not in ["thread_pool", "work_stealing"]:
        raise ValueError("pool_type must be 'thread_pool' or 'work_stealing'")

    _thread_pool_config = {
        "enabled": enabled,
        "max_workers": max_workers,
        "pool_type": pool_type,
    }

    logger.info(f"Thread pool configured: enabled={enabled}, max_workers={max_workers}, type={pool_type}")


def get_thread_pool_config() -> dict[str, Any]:
    """Get current thread pool configuration."""
    return _thread_pool_config.copy()


def get_thread_pool() -> Optional[LogicSpongeThreadPool]:
    """Get the global thread pool instance if enabled."""
    if not _thread_pool_config["enabled"]:
        return None

    return LogicSpongeThreadPool(max_workers=_thread_pool_config["max_workers"])


def shutdown_thread_pool():
    """Shutdown the global thread pool."""
    if LogicSpongeThreadPool._instance is not None:
        LogicSpongeThreadPool._instance.shutdown()
