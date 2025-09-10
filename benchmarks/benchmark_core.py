"""Benchmarks for logicsponge-core data streaming library."""

import time
import tempfile
import statistics
from typing import Any, Callable
from pathlib import Path
import threading
import queue as queue_module

import logicsponge.core as ls
from logicsponge.core.logicsponge import LatencyQueue
from logicsponge.core.datastructures import SharedQueue, SharedQueueView
from logicsponge.core.source import IterableSource, CSVStreamer


class BenchmarkRunner:
    """Utility class for running benchmarks."""

    def __init__(self, warmup_runs: int = 3, benchmark_runs: int = 10):
        """Initialize benchmark runner.

        Args:
            warmup_runs: Number of warmup runs to perform
            benchmark_runs: Number of benchmark runs for timing
        """
        self.warmup_runs = warmup_runs
        self.benchmark_runs = benchmark_runs

    def run_benchmark(self, func: Callable, *args, **kwargs) -> dict[str, float]:
        """Run a benchmark function and return timing statistics.

        Args:
            func: Function to benchmark
            *args: Arguments to pass to function
            **kwargs: Keyword arguments to pass to function

        Returns:
            Dictionary with timing statistics (mean, min, max, std)
        """
        # Warmup runs
        for _ in range(self.warmup_runs):
            func(*args, **kwargs)

        # Benchmark runs
        times = []
        for _ in range(self.benchmark_runs):
            start = time.perf_counter()
            result = func(*args, **kwargs)
            end = time.perf_counter()
            times.append(end - start)

        return {
            "mean": statistics.mean(times),
            "min": min(times),
            "max": max(times),
            "std": statistics.stdev(times) if len(times) > 1 else 0.0,
            "runs": len(times),
        }


class CoreBenchmarks:
    """Benchmarks for core data streaming components."""

    def __init__(self):
        """Initialize core benchmarks."""
        self.runner = BenchmarkRunner()

    def benchmark_data_item_creation(self, n_items: int = 10000) -> dict[str, float]:
        """Benchmark DataItem creation performance."""

        def create_data_items():
            for i in range(n_items):
                ls.DataItem({"id": i, "value": f"data_{i}", "timestamp": time.time()})

        return self.runner.run_benchmark(create_data_items)

    def benchmark_data_item_access(self, n_items: int = 10000) -> dict[str, float]:
        """Benchmark DataItem field access performance."""
        items = [ls.DataItem({"id": i, "value": f"data_{i}"}) for i in range(n_items)]

        def access_data_items():
            total = 0
            for item in items:
                total += item["id"]
            return total

        return self.runner.run_benchmark(access_data_items)

    def benchmark_latency_queue(self, n_operations: int = 1000) -> dict[str, float]:
        """Benchmark LatencyQueue performance."""

        def latency_operations():
            lq = LatencyQueue(max_size=100)
            for _ in range(n_operations):
                lq.tic()
                time.sleep(0.0001)  # Simulate small operation
                lq.toc()
            return lq.avg

        return self.runner.run_benchmark(latency_operations)


class SharedQueueBenchmarks:
    """Benchmarks for SharedQueue and SharedQueueView."""

    def __init__(self):
        """Initialize shared queue benchmarks."""
        self.runner = BenchmarkRunner()

    def benchmark_queue_append(self, n_items: int = 10000) -> dict[str, float]:
        """Benchmark SharedQueue append performance."""

        def append_items():
            queue: SharedQueue[int] = SharedQueue()
            for i in range(n_items):
                queue.append(i)
            return len(queue)

        return self.runner.run_benchmark(append_items)

    def benchmark_queue_access(self, n_items: int = 10000) -> dict[str, float]:
        """Benchmark SharedQueue random access performance."""
        queue: SharedQueue[int] = SharedQueue()
        for i in range(n_items):
            queue.append(i)

        def access_items():
            total = 0
            for i in range(0, n_items, 10):  # Sample every 10th item
                total += queue[i]
            return total

        return self.runner.run_benchmark(access_items)

    def benchmark_queue_view_creation(self, n_views: int = 100) -> dict[str, float]:
        """Benchmark SharedQueueView creation performance."""
        queue: SharedQueue[int] = SharedQueue()
        for i in range(1000):
            queue.append(i)

        def create_views():
            views = []
            for _ in range(n_views):
                views.append(queue.create_view())
            return len(views)

        return self.runner.run_benchmark(create_views)

    def benchmark_concurrent_access(self, n_threads: int = 4, n_items_per_thread: int = 1000) -> dict[str, float]:
        """Benchmark concurrent access to SharedQueue."""

        def concurrent_test():
            queue: SharedQueue[int] = SharedQueue()
            barrier = threading.Barrier(n_threads + 1)
            results = queue_module.Queue()

            def worker_append(start_idx: int):
                barrier.wait()  # Synchronize start
                worker_start = time.perf_counter()
                for i in range(start_idx, start_idx + n_items_per_thread):
                    queue.append(i)
                worker_end = time.perf_counter()
                results.put(worker_end - worker_start)

            # Start worker threads
            threads = []
            for i in range(n_threads):
                t = threading.Thread(target=worker_append, args=(i * n_items_per_thread,))
                threads.append(t)
                t.start()

            # Wait for all threads to be ready, then start timing
            start_time = time.perf_counter()
            barrier.wait()

            # Wait for all threads to complete
            for t in threads:
                t.join()
            end_time = time.perf_counter()

            return end_time - start_time

        return self.runner.run_benchmark(concurrent_test)


class SourceBenchmarks:
    """Benchmarks for data source components."""

    def __init__(self):
        """Initialize source benchmarks."""
        self.runner = BenchmarkRunner()

    def benchmark_iterable_source(self, n_items: int = 10000) -> dict[str, float]:
        """Benchmark IterableSource performance."""
        data = [{"id": i, "value": f"item_{i}"} for i in range(n_items)]

        def run_iterable_source():
            source = IterableSource(data, name="benchmark_source")

            # Manually iterate through the source's logic
            item_count = 0
            for item in data:
                formatted = source._formatter(item)
                data_item = ls.DataItem(formatted)
                item_count += 1

            return item_count

        return self.runner.run_benchmark(run_iterable_source)

    def benchmark_csv_creation(self, n_rows: int = 1000) -> dict[str, float]:
        """Benchmark CSV file creation and parsing setup."""

        def create_csv_and_source():
            # Create temporary CSV file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
                f.write("id,name,value\n")
                for i in range(n_rows):
                    f.write(f"{i},item_{i},{i * 2}\n")
                csv_path = f.name

            # Create CSV streamer (but don't run it)
            streamer = CSVStreamer(file_path=csv_path, poll_delay=0.1)

            # Cleanup
            Path(csv_path).unlink()

            return streamer

        return self.runner.run_benchmark(create_csv_and_source)


class StreamProcessingBenchmarks:
    """Benchmarks for stream processing operations."""

    def __init__(self):
        """Initialize stream processing benchmarks."""
        self.runner = BenchmarkRunner()

    def benchmark_function_term(self, n_items: int = 10000) -> dict[str, float]:
        """Benchmark FunctionTerm processing performance."""

        class SquareTerm(ls.FunctionTerm):
            def f(self, data: ls.DataItem) -> ls.DataItem:
                return ls.DataItem({**data, "squared": data["value"] ** 2})

        def process_items():
            items = [ls.DataItem({"value": i}) for i in range(n_items)]
            term = SquareTerm(name="square_term")

            results = []
            for item in items:
                result = term.f(item)
                results.append(result)

            return len(results)

        return self.runner.run_benchmark(process_items)

    def benchmark_filter_term(self, n_items: int = 10000) -> dict[str, float]:
        """Benchmark DataItemFilter processing performance."""

        class EvenFilterTerm(ls.DataItemFilter):
            def filter(self, data: ls.DataItem) -> bool:
                return data["value"] % 2 == 0

        def filter_items():
            items = [ls.DataItem({"value": i}) for i in range(n_items)]
            filter_term = EvenFilterTerm(name="even_filter")

            results = []
            for item in items:
                if filter_term.filter(item):
                    results.append(item)

            return len(results)

        return self.runner.run_benchmark(filter_items)


def run_all_benchmarks():
    """Run all benchmarks and print results."""
    print("=" * 60)
    print("LOGICSPONGE-CORE BENCHMARKS")
    print("=" * 60)

    # Core benchmarks
    print("\n--- Core Component Benchmarks ---")
    core_bench = CoreBenchmarks()

    print("DataItem creation (10K items):")
    result = core_bench.benchmark_data_item_creation()
    print(
        f"  Mean: {result['mean'] * 1000:.3f}ms, Min: {result['min'] * 1000:.3f}ms, Max: {result['max'] * 1000:.3f}ms"
    )

    print("DataItem access (10K items):")
    result = core_bench.benchmark_data_item_access()
    print(
        f"  Mean: {result['mean'] * 1000:.3f}ms, Min: {result['min'] * 1000:.3f}ms, Max: {result['max'] * 1000:.3f}ms"
    )

    print("LatencyQueue operations (1K ops):")
    result = core_bench.benchmark_latency_queue()
    print(
        f"  Mean: {result['mean'] * 1000:.3f}ms, Min: {result['min'] * 1000:.3f}ms, Max: {result['max'] * 1000:.3f}ms"
    )

    # SharedQueue benchmarks
    print("\n--- SharedQueue Benchmarks ---")
    queue_bench = SharedQueueBenchmarks()

    print("Queue append (10K items):")
    result = queue_bench.benchmark_queue_append()
    print(
        f"  Mean: {result['mean'] * 1000:.3f}ms, Min: {result['min'] * 1000:.3f}ms, Max: {result['max'] * 1000:.3f}ms"
    )

    print("Queue access (1K samples from 10K items):")
    result = queue_bench.benchmark_queue_access()
    print(
        f"  Mean: {result['mean'] * 1000:.3f}ms, Min: {result['min'] * 1000:.3f}ms, Max: {result['max'] * 1000:.3f}ms"
    )

    print("Queue view creation (100 views):")
    result = queue_bench.benchmark_queue_view_creation()
    print(
        f"  Mean: {result['mean'] * 1000:.3f}ms, Min: {result['min'] * 1000:.3f}ms, Max: {result['max'] * 1000:.3f}ms"
    )

    print("Concurrent access (4 threads, 1K items each):")
    result = queue_bench.benchmark_concurrent_access()
    print(
        f"  Mean: {result['mean'] * 1000:.3f}ms, Min: {result['min'] * 1000:.3f}ms, Max: {result['max'] * 1000:.3f}ms"
    )

    # Source benchmarks
    print("\n--- Source Component Benchmarks ---")
    source_bench = SourceBenchmarks()

    print("CSV creation and streamer setup (1K rows):")
    result = source_bench.benchmark_csv_creation()
    print(
        f"  Mean: {result['mean'] * 1000:.3f}ms, Min: {result['min'] * 1000:.3f}ms, Max: {result['max'] * 1000:.3f}ms"
    )

    # Stream processing benchmarks
    print("\n--- Stream Processing Benchmarks ---")
    processing_bench = StreamProcessingBenchmarks()

    print("FunctionTerm processing (10K items):")
    result = processing_bench.benchmark_function_term()
    print(
        f"  Mean: {result['mean'] * 1000:.3f}ms, Min: {result['min'] * 1000:.3f}ms, Max: {result['max'] * 1000:.3f}ms"
    )

    print("FilterTerm processing (10K items):")
    result = processing_bench.benchmark_filter_term()
    print(
        f"  Mean: {result['mean'] * 1000:.3f}ms, Min: {result['min'] * 1000:.3f}ms, Max: {result['max'] * 1000:.3f}ms"
    )

    print("\n" + "=" * 60)
    print("Benchmark completed!")


if __name__ == "__main__":
    run_all_benchmarks()
