"""Thread pool benchmark for logicsponge-core to compare threading models."""

import time
import threading
import concurrent.futures
import os
import sys
import psutil
from typing import Any, Callable
from queue import Queue, Empty
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logicsponge.core as ls
from logicsponge.core.source import IterableSource


class ThreadPoolManager:
    """Manages thread pool for logicsponge terms."""

    def __init__(self, max_workers: int = None):
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) * 2)

        self.max_workers = max_workers
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="logicsponge-pool"
        )
        self.active_tasks = 0
        self.task_lock = threading.Lock()

    def submit_term_work(self, work_func, *args, **kwargs):
        """Submit work to the thread pool."""
        with self.task_lock:
            self.active_tasks += 1

        def wrapper():
            try:
                return work_func(*args, **kwargs)
            finally:
                with self.task_lock:
                    self.active_tasks -= 1

        return self.executor.submit(wrapper)

    def get_utilization(self):
        """Get current thread pool utilization."""
        with self.task_lock:
            return self.active_tasks / self.max_workers

    def shutdown(self, wait=True):
        """Shutdown the thread pool."""
        if hasattr(self, "executor"):
            self.executor.shutdown(wait=wait)


class WorkStealingScheduler:
    """Work-stealing scheduler for comparison."""

    def __init__(self, num_workers: int = None):
        self.num_workers = num_workers or min(16, os.cpu_count() * 2)
        self.work_queues = [Queue() for _ in range(self.num_workers)]
        self.workers = []
        self.running = False
        self.completed_tasks = 0
        self.task_lock = threading.Lock()

    def start(self):
        """Start worker threads."""
        self.running = True
        for i in range(self.num_workers):
            worker = threading.Thread(target=self._worker_loop, args=(i,), name=f"work-stealer-{i}", daemon=True)
            worker.start()
            self.workers.append(worker)

    def _worker_loop(self, worker_id: int):
        """Main worker loop with work stealing."""
        my_queue = self.work_queues[worker_id]

        while self.running:
            task_found = False

            # Try own queue first
            try:
                task = my_queue.get_nowait()
                task()
                with self.task_lock:
                    self.completed_tasks += 1
                task_found = True
                continue
            except Empty:
                pass

            # Steal from other queues
            for i, queue in enumerate(self.work_queues):
                if i != worker_id:
                    try:
                        task = queue.get_nowait()
                        task()
                        with self.task_lock:
                            self.completed_tasks += 1
                        task_found = True
                        break
                    except Empty:
                        continue

            if not task_found:
                time.sleep(0.001)  # Brief sleep if no work found

    def submit(self, task):
        """Submit task to least loaded queue."""
        min_queue = min(self.work_queues, key=lambda q: q.qsize())
        min_queue.put(task)

    def shutdown(self):
        """Shutdown the scheduler."""
        self.running = False
        for worker in self.workers:
            worker.join(timeout=1.0)


class ThreadingModelBenchmarks:
    """Compare different threading models for logicsponge."""

    @staticmethod
    def benchmark_current_model(n_terms: int = 50, n_items: int = 1000) -> dict[str, Any]:
        """Benchmark current dedicated-thread-per-term model."""
        inputs = [{"id": i, "value": i} for i in range(n_items)]
        results = []
        results_lock = threading.Lock()

        def collect_result(item):
            with results_lock:
                results.append(item)

        source = IterableSource(inputs, name="current_source")

        class CurrentMultiplierTerm(ls.FunctionTerm):
            def __init__(self, multiplier: int):
                super().__init__(name=f"current_mult_{multiplier}")
                self.multiplier = multiplier

            def f(self, data: ls.DataItem) -> ls.DataItem:
                return ls.DataItem({**data, "value": data["value"] * self.multiplier})

        # Create parallel terms (current model)
        parallel_terms = []
        for i in range(n_terms):
            branch = CurrentMultiplierTerm(multiplier=i + 1) * ls.Dump(
                print_fun=collect_result, name=f"current_dump_{i}"
            )
            parallel_terms.append(branch)

        # Build pipeline
        if len(parallel_terms) == 1:
            pipeline = source * parallel_terms[0]
        else:
            parallel_section = parallel_terms[0]
            for term in parallel_terms[1:]:
                parallel_section = parallel_section | term
            pipeline = source * parallel_section

        # Monitor resources
        process = psutil.Process(os.getpid())
        initial_threads = process.num_threads()
        initial_memory = process.memory_info().rss / 1024 / 1024

        # Run benchmark
        start_time = time.perf_counter()
        pipeline.start()

        peak_threads = process.num_threads()
        peak_memory = process.memory_info().rss / 1024 / 1024

        pipeline.join()
        end_time = time.perf_counter()

        final_threads = process.num_threads()

        return {
            "model": "current_dedicated_threads",
            "n_terms": n_terms,
            "n_items": n_items,
            "items_output": len(results),
            "duration": end_time - start_time,
            "throughput": len(results) / (end_time - start_time),
            "initial_threads": initial_threads,
            "peak_threads": peak_threads,
            "final_threads": final_threads,
            "thread_overhead": peak_threads - initial_threads,
            "memory_overhead_mb": peak_memory - initial_memory,
        }

    @staticmethod
    def benchmark_thread_pool_simulation(n_terms: int = 50, n_items: int = 1000, pool_size: int = 16) -> dict[str, Any]:
        """Simulate thread pool model by processing terms in batches."""
        inputs = [{"id": i, "value": i} for i in range(n_items)]
        results = []

        # Create thread pool
        pool_manager = ThreadPoolManager(max_workers=pool_size)

        def process_term_batch(term_id: int, multiplier: int):
            """Simulate processing a term's work in thread pool."""
            batch_results = []
            for item in inputs:
                processed = {**item, "value": item["value"] * multiplier, "term_id": term_id}
                batch_results.append(processed)
            return batch_results

        # Monitor resources
        process = psutil.Process(os.getpid())
        initial_threads = process.num_threads()
        initial_memory = process.memory_info().rss / 1024 / 1024

        # Submit all terms to thread pool
        start_time = time.perf_counter()

        futures = []
        for i in range(n_terms):
            future = pool_manager.submit_term_work(process_term_batch, i, i + 1)
            futures.append(future)

        peak_threads = process.num_threads()
        peak_memory = process.memory_info().rss / 1024 / 1024
        max_utilization = 0

        # Wait for completion and collect results
        for future in concurrent.futures.as_completed(futures):
            batch_results = future.result()
            results.extend(batch_results)
            utilization = pool_manager.get_utilization()
            max_utilization = max(max_utilization, utilization)

        end_time = time.perf_counter()
        final_threads = process.num_threads()

        # Cleanup
        pool_manager.shutdown()

        return {
            "model": "thread_pool_simulation",
            "n_terms": n_terms,
            "n_items": n_items,
            "pool_size": pool_size,
            "items_output": len(results),
            "duration": end_time - start_time,
            "throughput": len(results) / (end_time - start_time),
            "initial_threads": initial_threads,
            "peak_threads": peak_threads,
            "final_threads": final_threads,
            "thread_overhead": peak_threads - initial_threads,
            "memory_overhead_mb": peak_memory - initial_memory,
            "max_pool_utilization": max_utilization,
        }

    @staticmethod
    def benchmark_work_stealing(n_terms: int = 50, n_items: int = 1000, num_workers: int = 16) -> dict[str, Any]:
        """Benchmark work-stealing scheduler."""
        inputs = [{"id": i, "value": i} for i in range(n_items)]
        results = []
        results_lock = threading.Lock()

        # Create work-stealing scheduler
        scheduler = WorkStealingScheduler(num_workers=num_workers)

        def process_term(term_id: int, multiplier: int):
            """Process one term's work."""
            batch_results = []
            for item in inputs:
                processed = {**item, "value": item["value"] * multiplier, "term_id": term_id}
                batch_results.append(processed)

            with results_lock:
                results.extend(batch_results)

        # Monitor resources
        process = psutil.Process(os.getpid())
        initial_threads = process.num_threads()
        initial_memory = process.memory_info().rss / 1024 / 1024

        # Start scheduler and submit work
        scheduler.start()
        start_time = time.perf_counter()

        for i in range(n_terms):
            task = lambda term_id=i, mult=i + 1: process_term(term_id, mult)
            scheduler.submit(task)

        peak_threads = process.num_threads()
        peak_memory = process.memory_info().rss / 1024 / 1024

        # Wait for completion
        while scheduler.completed_tasks < n_terms:
            time.sleep(0.001)

        end_time = time.perf_counter()
        final_threads = process.num_threads()

        # Cleanup
        scheduler.shutdown()

        return {
            "model": "work_stealing",
            "n_terms": n_terms,
            "n_items": n_items,
            "num_workers": num_workers,
            "items_output": len(results),
            "duration": end_time - start_time,
            "throughput": len(results) / (end_time - start_time),
            "initial_threads": initial_threads,
            "peak_threads": peak_threads,
            "final_threads": final_threads,
            "thread_overhead": peak_threads - initial_threads,
            "memory_overhead_mb": peak_memory - initial_memory,
            "completed_tasks": scheduler.completed_tasks,
        }

    @staticmethod
    def compare_threading_models(n_terms: int = 100, n_items: int = 1000) -> dict[str, Any]:
        """Compare all threading models side by side."""

        print(f"  Testing current model ({n_terms} terms)...")
        current_result = ThreadingModelBenchmarks.benchmark_current_model(n_terms, n_items)

        # Test different pool sizes
        pool_sizes = [8, 16, 32]
        pool_results = []

        for pool_size in pool_sizes:
            print(f"  Testing thread pool (size {pool_size})...")
            result = ThreadingModelBenchmarks.benchmark_thread_pool_simulation(n_terms, n_items, pool_size)
            pool_results.append(result)

        print(f"  Testing work stealing (16 workers)...")
        work_stealing_result = ThreadingModelBenchmarks.benchmark_work_stealing(n_terms, n_items, 16)

        # Find best performing model
        all_results = [current_result] + pool_results + [work_stealing_result]
        best_throughput = max(all_results, key=lambda x: x["throughput"])
        least_threads = min(all_results, key=lambda x: x["thread_overhead"])
        least_memory = min(all_results, key=lambda x: x["memory_overhead_mb"])

        return {
            "n_terms": n_terms,
            "n_items": n_items,
            "current_model": current_result,
            "thread_pool_results": pool_results,
            "work_stealing_result": work_stealing_result,
            "best_throughput": best_throughput,
            "least_threads": least_threads,
            "least_memory": least_memory,
            "throughput_improvement": (best_throughput["throughput"] - current_result["throughput"])
            / current_result["throughput"]
            * 100,
            "thread_reduction": (current_result["thread_overhead"] - least_threads["thread_overhead"])
            / current_result["thread_overhead"]
            * 100,
            "memory_reduction": (current_result["memory_overhead_mb"] - least_memory["memory_overhead_mb"])
            / current_result["memory_overhead_mb"]
            * 100
            if current_result["memory_overhead_mb"] > 0
            else 0,
        }


def run_thread_pool_benchmarks():
    """Run thread pool comparison benchmarks."""
    print("=" * 70)
    print("LOGICSPONGE-CORE THREAD POOL BENCHMARKS")
    print("=" * 70)

    # Compare models with moderate term count
    print("\n--- Threading Model Comparison (100 terms) ---")
    result = ThreadingModelBenchmarks.compare_threading_models(100, 1000)

    print(f"Terms: {result['n_terms']}, Items: {result['n_items']:,}")
    print()

    # Current model results
    current = result["current_model"]
    print("Current Model (dedicated threads):")
    print(f"  Throughput: {current['throughput']:,.0f} items/sec")
    print(f"  Thread overhead: {current['thread_overhead']} threads")
    print(f"  Memory overhead: {current['memory_overhead_mb']:.1f} MB")
    print(f"  Duration: {current['duration']:.3f}s")

    # Thread pool results
    print("\nThread Pool Models:")
    for pool_result in result["thread_pool_results"]:
        print(f"  Pool size {pool_result['pool_size']}:")
        print(f"    Throughput: {pool_result['throughput']:,.0f} items/sec")
        print(f"    Thread overhead: {pool_result['thread_overhead']} threads")
        print(f"    Memory overhead: {pool_result['memory_overhead_mb']:.1f} MB")
        print(f"    Duration: {pool_result['duration']:.3f}s")
        print(f"    Max utilization: {pool_result['max_pool_utilization']:.1%}")

    # Work stealing results
    ws = result["work_stealing_result"]
    print(f"\nWork Stealing Model ({ws['num_workers']} workers):")
    print(f"  Throughput: {ws['throughput']:,.0f} items/sec")
    print(f"  Thread overhead: {ws['thread_overhead']} threads")
    print(f"  Memory overhead: {ws['memory_overhead_mb']:.1f} MB")
    print(f"  Duration: {ws['duration']:.3f}s")
    print(f"  Tasks completed: {ws['completed_tasks']}")

    # Best results summary
    print("\n--- Performance Summary ---")
    print(
        f"Best throughput: {result['best_throughput']['model']} ({result['best_throughput']['throughput']:,.0f} items/sec)"
    )
    print(f"Least threads: {result['least_threads']['model']} ({result['least_threads']['thread_overhead']} threads)")
    print(f"Least memory: {result['least_memory']['model']} ({result['least_memory']['memory_overhead_mb']:.1f} MB)")
    print()
    print(f"Throughput improvement: {result['throughput_improvement']:+.1f}%")
    print(f"Thread reduction: {result['thread_reduction']:+.1f}%")
    print(f"Memory reduction: {result['memory_reduction']:+.1f}%")

    print("\n" + "=" * 70)
    print("Thread pool benchmarks completed!")


if __name__ == "__main__":
    run_thread_pool_benchmarks()
