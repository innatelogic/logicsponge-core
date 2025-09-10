"""Throughput benchmarks for logicsponge-core streaming operations."""

import time
import threading
import tempfile
from pathlib import Path
from typing import Any
import csv

import logicsponge.core as ls
from logicsponge.core.source import IterableSource


class ThroughputCollector:
    """Collects items and measures throughput."""

    def __init__(self):
        """Initialize throughput collector."""
        self.items = []
        self.start_time = None
        self.end_time = None
        self.lock = threading.Lock()

    def start(self):
        """Start collecting."""
        self.start_time = time.perf_counter()

    def collect(self, item: ls.DataItem):
        """Collect an item."""
        with self.lock:
            if self.start_time is None:
                self.start_time = time.perf_counter()
            self.items.append(item)

    def stop(self):
        """Stop collecting and calculate throughput."""
        self.end_time = time.perf_counter()

    @property
    def throughput_items_per_second(self) -> float:
        """Calculate throughput in items per second."""
        if self.start_time is None or self.end_time is None:
            return 0.0

        duration = self.end_time - self.start_time
        if duration <= 0:
            return 0.0

        return len(self.items) / duration

    @property
    def total_items(self) -> int:
        """Get total number of items collected."""
        return len(self.items)

    @property
    def duration_seconds(self) -> float:
        """Get total duration in seconds."""
        if self.start_time is None or self.end_time is None:
            return 0.0
        return self.end_time - self.start_time


class DataStreamThroughputBenchmarks:
    """Throughput benchmarks for data streaming operations."""

    @staticmethod
    def benchmark_iterable_source_throughput(n_items: int = 100000) -> dict[str, Any]:
        """Benchmark IterableSource throughput."""
        # Create test data
        data = [{"id": i, "value": f"item_{i}", "timestamp": i} for i in range(n_items)]

        # Create collector
        collector = ThroughputCollector()

        def run_source():
            # Create and configure source
            source = IterableSource(data, name="throughput_test")

            # Count items during run
            item_count = 0
            for item in data:
                formatted = source._formatter(item)
                data_item = ls.DataItem(formatted)
                collector.collect(data_item)
                item_count += 1

            return item_count

        # Run the test
        collector.start()
        items_processed = run_source()
        collector.stop()

        return {
            "items_processed": collector.total_items,
            "duration_seconds": collector.duration_seconds,
            "throughput_items_per_sec": collector.throughput_items_per_second,
            "throughput_mb_per_sec": collector.throughput_items_per_second
            * 50
            / (1024 * 1024),  # Estimate 50 bytes per item
        }

    @staticmethod
    def benchmark_function_term_throughput(n_items: int = 100000) -> dict[str, Any]:
        """Benchmark FunctionTerm processing throughput."""

        class ProcessingTerm(ls.FunctionTerm):
            def __init__(self, collector: ThroughputCollector):
                super().__init__(name="processing_term")
                self.collector = collector

            def f(self, data: ls.DataItem) -> ls.DataItem:
                # Simulate processing
                processed = ls.DataItem(
                    {
                        **data,
                        "processed": True,
                        "doubled_value": data["value"] * 2 if isinstance(data["value"], int) else 0,
                    }
                )
                self.collector.collect(processed)
                return processed

        # Create test data
        items = [ls.DataItem({"id": i, "value": i, "data": f"item_{i}"}) for i in range(n_items)]

        # Create collector and term
        collector = ThroughputCollector()
        term = ProcessingTerm(collector)

        # Process items
        collector.start()
        for item in items:
            term.f(item)
        collector.stop()

        return {
            "items_processed": collector.total_items,
            "duration_seconds": collector.duration_seconds,
            "throughput_items_per_sec": collector.throughput_items_per_second,
        }

    @staticmethod
    def benchmark_filter_term_throughput(n_items: int = 100000, filter_ratio: float = 0.5) -> dict[str, Any]:
        """Benchmark DataItemFilter processing throughput."""

        class SelectiveTerm(ls.DataItemFilter):
            def __init__(self, collector: ThroughputCollector, threshold: int):
                super().__init__(name="selective_term")
                self.collector = collector
                self.threshold = threshold

            def filter(self, data: ls.DataItem) -> bool:
                should_pass = data["value"] >= self.threshold
                if should_pass:
                    self.collector.collect(data)
                return should_pass

        # Create test data
        items = [ls.DataItem({"id": i, "value": i}) for i in range(n_items)]
        threshold = int(n_items * (1 - filter_ratio))

        # Create collector and term
        collector = ThroughputCollector()
        term = SelectiveTerm(collector, threshold)

        # Process items
        collector.start()
        for item in items:
            term.filter(item)
        collector.stop()

        return {
            "items_input": n_items,
            "items_passed": collector.total_items,
            "filter_ratio": collector.total_items / n_items,
            "duration_seconds": collector.duration_seconds,
            "throughput_items_per_sec": n_items / collector.duration_seconds if collector.duration_seconds > 0 else 0,
        }


class ConcurrentThroughputBenchmarks:
    """Benchmarks for concurrent streaming operations."""

    @staticmethod
    def benchmark_concurrent_producers(n_producers: int = 4, items_per_producer: int = 25000) -> dict[str, Any]:
        """Benchmark multiple concurrent data producers."""
        from logicsponge.core.datastructures import SharedQueue

        queue: SharedQueue[ls.DataItem] = SharedQueue()
        barrier = threading.Barrier(n_producers + 1)

        def producer(producer_id: int):
            barrier.wait()  # Synchronize start
            for i in range(items_per_producer):
                item = ls.DataItem(
                    {"producer_id": producer_id, "item_id": i, "data": f"producer_{producer_id}_item_{i}"}
                )
                queue.append(item)

        # Start all producers
        threads = []
        for i in range(n_producers):
            t = threading.Thread(target=producer, args=(i,))
            threads.append(t)
            t.start()

        # Start timing
        start_time = time.perf_counter()
        barrier.wait()

        # Wait for all producers to finish
        for t in threads:
            t.join()

        end_time = time.perf_counter()
        duration = end_time - start_time
        total_items = len(queue)

        return {
            "producers": n_producers,
            "items_per_producer": items_per_producer,
            "total_items_produced": total_items,
            "duration_seconds": duration,
            "throughput_items_per_sec": total_items / duration if duration > 0 else 0,
            "per_producer_throughput": (total_items / n_producers) / duration if duration > 0 else 0,
        }

    @staticmethod
    def benchmark_simple_pipeline(n_items: int = 100000) -> dict[str, Any]:
        """Benchmark simple data pipeline processing."""

        def process_pipeline():
            # Create data
            items = [ls.DataItem({"id": i, "value": i * 2}) for i in range(n_items)]

            # Processing pipeline: filter even numbers, then square them
            results = []
            for item in items:
                if item["value"] % 2 == 0:  # Filter step
                    processed = ls.DataItem({**item, "squared": item["value"] ** 2, "processed": True})
                    results.append(processed)

            return len(results)

        # Time the pipeline
        start_time = time.perf_counter()
        items_processed = process_pipeline()
        end_time = time.perf_counter()
        duration = end_time - start_time

        return {
            "items_input": n_items,
            "items_processed": items_processed,
            "duration_seconds": duration,
            "throughput_items_per_sec": items_processed / duration if duration > 0 else 0,
            "processing_efficiency": items_processed / n_items if n_items > 0 else 0,
        }


def run_throughput_benchmarks():
    """Run all throughput benchmarks and print results."""
    print("=" * 70)
    print("LOGICSPONGE-CORE THROUGHPUT BENCHMARKS")
    print("=" * 70)

    # Data streaming throughput
    print("\n--- Data Streaming Throughput ---")

    print("IterableSource throughput (100K items):")
    result = DataStreamThroughputBenchmarks.benchmark_iterable_source_throughput(100000)
    print(f"  Items processed: {result['items_processed']:,}")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")
    print(f"  Estimated: {result['throughput_mb_per_sec']:.2f} MB/sec")

    print("\nFunctionTerm processing throughput (100K items):")
    result = DataStreamThroughputBenchmarks.benchmark_function_term_throughput(100000)
    print(f"  Items processed: {result['items_processed']:,}")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")

    print("\nFilterTerm processing throughput (100K items, 50% pass rate):")
    result = DataStreamThroughputBenchmarks.benchmark_filter_term_throughput(100000, 0.5)
    print(f"  Items input: {result['items_input']:,}")
    print(f"  Items passed: {result['items_passed']:,}")
    print(f"  Filter ratio: {result['filter_ratio']:.1%}")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")

    # Concurrent throughput
    print("\n--- Concurrent Throughput ---")

    print("Concurrent producers (4 producers, 25K items each):")
    result = ConcurrentThroughputBenchmarks.benchmark_concurrent_producers(4, 25000)
    print(f"  Producers: {result['producers']}")
    print(f"  Total items: {result['total_items_produced']:,}")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Overall throughput: {result['throughput_items_per_sec']:,.0f} items/sec")
    print(f"  Per-producer throughput: {result['per_producer_throughput']:,.0f} items/sec")

    print("\nSimple pipeline processing (100K items):")
    result = ConcurrentThroughputBenchmarks.benchmark_simple_pipeline(100000)
    print(f"  Items input: {result['items_input']:,}")
    print(f"  Items processed: {result['items_processed']:,}")
    print(f"  Processing efficiency: {result['processing_efficiency']:.1%}")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")

    print("\n" + "=" * 70)
    print("Throughput benchmark completed!")


if __name__ == "__main__":
    run_throughput_benchmarks()
