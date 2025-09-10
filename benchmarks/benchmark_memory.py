"""Memory usage benchmarks for logicsponge-core."""

import gc
import sys
import tracemalloc
from typing import Any, Callable

import logicsponge.core as ls
from logicsponge.core.logicsponge import LatencyQueue
from logicsponge.core.datastructures import SharedQueue


class MemoryBenchmark:
    """Utility class for memory benchmarks."""

    @staticmethod
    def measure_memory(func: Callable, *args, **kwargs) -> tuple[Any, dict[str, float]]:
        """Measure memory usage of a function.

        Args:
            func: Function to measure
            *args: Arguments to pass to function
            **kwargs: Keyword arguments to pass to function

        Returns:
            Tuple of (function_result, memory_stats)
        """
        # Force garbage collection before measurement
        gc.collect()

        # Start tracing
        tracemalloc.start()

        # Run function
        result = func(*args, **kwargs)

        # Get memory statistics
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Force garbage collection after measurement
        gc.collect()

        return result, {
            "current_mb": current / 1024 / 1024,
            "peak_mb": peak / 1024 / 1024,
            "current_bytes": current,
            "peak_bytes": peak,
        }


class DataItemMemoryBenchmarks:
    """Memory benchmarks for DataItem objects."""

    @staticmethod
    def benchmark_data_item_creation(n_items: int = 10000) -> dict[str, float]:
        """Benchmark memory usage for DataItem creation."""

        def create_items():
            items = []
            for i in range(n_items):
                item = ls.DataItem(
                    {
                        "id": i,
                        "value": f"data_item_{i}",
                        "timestamp": i * 1000,
                        "metadata": {"created": True, "index": i},
                    }
                )
                items.append(item)
            return items

        result, memory_stats = MemoryBenchmark.measure_memory(create_items)

        # Calculate per-item memory usage
        memory_stats["per_item_bytes"] = memory_stats["peak_bytes"] / n_items
        memory_stats["per_item_kb"] = memory_stats["per_item_bytes"] / 1024
        memory_stats["total_items"] = len(result)

        return memory_stats

    @staticmethod
    def benchmark_data_item_growth(max_fields: int = 100) -> dict[str, float]:
        """Benchmark memory growth with increasing DataItem field count."""

        def create_growing_items():
            items = []
            for field_count in range(1, max_fields + 1):
                data = {f"field_{i}": f"value_{i}" for i in range(field_count)}
                items.append(ls.DataItem(data))
            return items

        result, memory_stats = MemoryBenchmark.measure_memory(create_growing_items)

        memory_stats["items_created"] = len(result)
        memory_stats["max_fields"] = max_fields

        return memory_stats


class SharedQueueMemoryBenchmarks:
    """Memory benchmarks for SharedQueue operations."""

    @staticmethod
    def benchmark_queue_memory_usage(n_items: int = 10000) -> dict[str, float]:
        """Benchmark SharedQueue memory usage."""

        def create_and_fill_queue():
            queue: SharedQueue[dict] = SharedQueue()
            for i in range(n_items):
                queue.append({"id": i, "data": f"item_{i}", "value": i * 2})
            return queue

        result, memory_stats = MemoryBenchmark.measure_memory(create_and_fill_queue)

        memory_stats["per_item_bytes"] = memory_stats["peak_bytes"] / n_items
        memory_stats["queue_length"] = len(result)

        return memory_stats

    @staticmethod
    def benchmark_queue_views_memory(n_views: int = 100) -> dict[str, float]:
        """Benchmark memory usage with multiple queue views."""

        def create_queue_with_views():
            queue: SharedQueue[int] = SharedQueue()

            # Add some data to the queue
            for i in range(1000):
                queue.append(i)

            # Create multiple views
            views = []
            for _ in range(n_views):
                views.append(queue.create_view())

            return queue, views

        result, memory_stats = MemoryBenchmark.measure_memory(create_queue_with_views)

        queue, views = result
        memory_stats["queue_length"] = len(queue)
        memory_stats["view_count"] = len(views)
        memory_stats["per_view_bytes"] = memory_stats["peak_bytes"] / n_views if n_views > 0 else 0

        return memory_stats


class LatencyQueueMemoryBenchmarks:
    """Memory benchmarks for LatencyQueue."""

    @staticmethod
    def benchmark_latency_queue_memory(queue_size: int = 1000, n_operations: int = 10000) -> dict[str, float]:
        """Benchmark LatencyQueue memory usage with operations."""

        def create_and_use_latency_queue():
            lq = LatencyQueue(max_size=queue_size)

            # Perform operations to fill the queue
            for i in range(n_operations):
                lq.tic()
                # Simulate some work
                sum(range(10))  # Small computation
                lq.toc()

            return lq

        result, memory_stats = MemoryBenchmark.measure_memory(create_and_use_latency_queue)

        memory_stats["queue_size"] = queue_size
        memory_stats["operations"] = n_operations
        memory_stats["actual_queue_len"] = len(result.queue)

        return memory_stats


def run_memory_benchmarks():
    """Run all memory benchmarks and print results."""
    print("=" * 60)
    print("LOGICSPONGE-CORE MEMORY BENCHMARKS")
    print("=" * 60)

    # DataItem memory benchmarks
    print("\n--- DataItem Memory Usage ---")

    print("DataItem creation (10K items):")
    result = DataItemMemoryBenchmarks.benchmark_data_item_creation(10000)
    print(f"  Peak memory: {result['peak_mb']:.2f} MB")
    print(f"  Per item: {result['per_item_kb']:.3f} KB")
    print(f"  Total items: {result['total_items']}")

    print("\nDataItem field growth (up to 100 fields):")
    result = DataItemMemoryBenchmarks.benchmark_data_item_growth(100)
    print(f"  Peak memory: {result['peak_mb']:.2f} MB")
    print(f"  Items created: {result['items_created']}")

    # SharedQueue memory benchmarks
    print("\n--- SharedQueue Memory Usage ---")

    print("SharedQueue with 10K items:")
    result = SharedQueueMemoryBenchmarks.benchmark_queue_memory_usage(10000)
    print(f"  Peak memory: {result['peak_mb']:.2f} MB")
    print(f"  Per item: {result['per_item_bytes']:.1f} bytes")
    print(f"  Queue length: {result['queue_length']}")

    print("\nSharedQueue with 100 views:")
    result = SharedQueueMemoryBenchmarks.benchmark_queue_views_memory(100)
    print(f"  Peak memory: {result['peak_mb']:.2f} MB")
    print(f"  View count: {result['view_count']}")
    print(f"  Queue length: {result['queue_length']}")

    # LatencyQueue memory benchmarks
    print("\n--- LatencyQueue Memory Usage ---")

    print("LatencyQueue (1K size, 10K operations):")
    result = LatencyQueueMemoryBenchmarks.benchmark_latency_queue_memory(1000, 10000)
    print(f"  Peak memory: {result['peak_mb']:.2f} MB")
    print(f"  Queue size limit: {result['queue_size']}")
    print(f"  Operations performed: {result['operations']}")
    print(f"  Actual queue length: {result['actual_queue_len']}")

    print("\n" + "=" * 60)
    print("Memory benchmark completed!")
    print(f"Python version: {sys.version}")


if __name__ == "__main__":
    run_memory_benchmarks()
