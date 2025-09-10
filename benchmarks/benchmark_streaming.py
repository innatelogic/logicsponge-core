"""Real streaming pipeline benchmarks for logicsponge-core."""

import time
import threading
from typing import Any

import logicsponge.core as ls
from logicsponge.core.source import IterableSource


class StreamingPipelineBenchmarks:
    """Benchmarks for actual streaming pipelines using start() and join()."""

    @staticmethod
    def benchmark_simple_streaming_pipeline(n_items: int = 10000) -> dict[str, Any]:
        """Benchmark a simple source -> processing -> dump pipeline."""
        # Create input data
        inputs = [{"id": i, "value": i * 2, "name": f"item_{i}"} for i in range(n_items)]

        # Create results collector
        results = []

        def collect_result(item):
            results.append(item)

        # Create pipeline: source -> processing -> dump
        source = IterableSource(inputs, name="benchmark_source")

        class DoublerTerm(ls.FunctionTerm):
            def f(self, data: ls.DataItem) -> ls.DataItem:
                return ls.DataItem({**data, "doubled": data["value"] * 2, "processed": True})

        processor = DoublerTerm(name="doubler")
        dumper = ls.Dump(print_fun=collect_result)

        # Build the pipeline
        sponge = source * processor * dumper

        # Benchmark the pipeline
        start_time = time.perf_counter()
        sponge.start()
        sponge.join()
        end_time = time.perf_counter()

        duration = end_time - start_time

        return {
            "items_input": n_items,
            "items_output": len(results),
            "duration_seconds": duration,
            "throughput_items_per_sec": len(results) / duration if duration > 0 else 0,
            "pipeline_stages": 3,  # source -> processor -> dump
        }

    @staticmethod
    def benchmark_parallel_pipeline(n_items: int = 10000) -> dict[str, Any]:
        """Benchmark parallel processing pipeline using | operator."""
        # Create input data
        inputs = [{"id": i, "value": i} for i in range(n_items)]

        # Create results collectors
        even_results = []
        odd_results = []

        def collect_even(item):
            even_results.append(item)

        def collect_odd(item):
            odd_results.append(item)

        # Create pipeline components
        source = IterableSource(inputs, name="parallel_source")

        # Parallel branches: filter even/odd numbers
        even_filter = ls.KeyValueFilter(key="value", predicate=lambda x: x % 2 == 0, name="even_filter") * ls.Dump(
            print_fun=collect_even, name="even_dump"
        )

        odd_filter = ls.KeyValueFilter(key="value", predicate=lambda x: x % 2 == 1, name="odd_filter") * ls.Dump(
            print_fun=collect_odd, name="odd_dump"
        )

        # Build parallel pipeline: source -> (even_branch | odd_branch)
        sponge = source * (even_filter | odd_filter)

        # Benchmark the pipeline
        start_time = time.perf_counter()
        sponge.start()
        sponge.join()
        end_time = time.perf_counter()

        duration = end_time - start_time

        return {
            "items_input": n_items,
            "even_items": len(even_results),
            "odd_items": len(odd_results),
            "total_output": len(even_results) + len(odd_results),
            "duration_seconds": duration,
            "throughput_items_per_sec": (len(even_results) + len(odd_results)) / duration if duration > 0 else 0,
            "parallel_branches": 2,
        }

    @staticmethod
    def benchmark_complex_processing_pipeline(n_items: int = 10000) -> dict[str, Any]:
        """Benchmark complex multi-stage processing pipeline."""
        # Create input data
        inputs = [{"id": i, "value": i, "category": "A" if i % 3 == 0 else "B"} for i in range(n_items)]

        # Create results collector
        results = []

        def collect_result(item):
            results.append(item)

        # Create processing pipeline components
        source = IterableSource(inputs, name="complex_source")

        # Processing stages
        class ComputeTerm(ls.FunctionTerm):
            def f(self, data: ls.DataItem) -> ls.DataItem:
                return ls.DataItem({**data, "squared": data["value"] ** 2})

        class TimestampTerm(ls.FunctionTerm):
            def f(self, data: ls.DataItem) -> ls.DataItem:
                return ls.DataItem({**data, "timestamp": time.time()})

        class FinalizeTerm(ls.FunctionTerm):
            def f(self, data: ls.DataItem) -> ls.DataItem:
                return ls.DataItem({**data, "final_score": data["squared"] + data["id"], "processed": True})

        # Stage 1: Add computed field
        stage1 = ComputeTerm(name="compute")

        # Stage 2: Filter only category A items
        stage2 = ls.KeyValueFilter(key="category", predicate=lambda x: x == "A", name="category_filter")

        # Stage 3: Add timestamp
        stage3 = TimestampTerm(name="timestamp")

        # Stage 4: Final processing
        stage4 = FinalizeTerm(name="finalize")

        # Stage 5: Dump results
        dumper = ls.Dump(print_fun=collect_result)

        # Build the complex pipeline
        sponge = source * stage1 * stage2 * stage3 * stage4 * dumper

        # Benchmark the pipeline
        start_time = time.perf_counter()
        sponge.start()
        sponge.join()
        end_time = time.perf_counter()

        duration = end_time - start_time

        return {
            "items_input": n_items,
            "items_output": len(results),
            "duration_seconds": duration,
            "throughput_items_per_sec": len(results) / duration if duration > 0 else 0,
            "pipeline_stages": 6,  # source + 4 processing + dump
            "processing_efficiency": len(results) / n_items if n_items > 0 else 0,
        }

    @staticmethod
    def benchmark_streaming_with_delay(n_items: int = 1000, delay_ms: int = 1) -> dict[str, Any]:
        """Benchmark streaming pipeline with processing delays."""
        # Create input data
        inputs = [{"id": i, "data": f"message_{i}"} for i in range(n_items)]

        # Create results collector
        results = []

        def collect_result(item):
            results.append(item)

        # Create pipeline components
        source = IterableSource(inputs, name="delayed_source")

        # Add artificial delay to simulate real processing
        class DelayedProcessor(ls.FunctionTerm):
            def __init__(self, delay_seconds: float):
                super().__init__(name="delayed_processor")
                self.delay_seconds = delay_seconds

            def f(self, data: ls.DataItem) -> ls.DataItem:
                time.sleep(self.delay_seconds)
                return ls.DataItem({**data, "processed_at": time.time(), "delayed": True})

        processor = DelayedProcessor(delay_ms / 1000.0)

        dumper = ls.Dump(print_fun=collect_result)

        # Build pipeline
        sponge = source * processor * dumper

        # Benchmark the pipeline
        start_time = time.perf_counter()
        sponge.start()
        sponge.join()
        end_time = time.perf_counter()

        duration = end_time - start_time

        return {
            "items_input": n_items,
            "items_output": len(results),
            "delay_per_item_ms": delay_ms,
            "duration_seconds": duration,
            "throughput_items_per_sec": len(results) / duration if duration > 0 else 0,
            "expected_min_duration": (n_items * delay_ms) / 1000.0,
            "overhead_ratio": duration / ((n_items * delay_ms) / 1000.0) if delay_ms > 0 else 1.0,
        }

    @staticmethod
    def benchmark_merge_streams_pipeline(n_items_per_stream: int = 5000) -> dict[str, Any]:
        """Benchmark merging multiple streams into a single output."""
        # Create multiple input streams
        stream1_data = [{"id": i, "stream": "A", "value": i * 2} for i in range(n_items_per_stream)]
        stream2_data = [{"id": i, "stream": "B", "value": i * 3} for i in range(n_items_per_stream)]

        # Create results collector
        results = []

        def collect_result(item):
            results.append(item)

        # Create sources
        source1 = IterableSource(stream1_data, name="stream_A")
        source2 = IterableSource(stream2_data, name="stream_B")

        # Create merger and dumper
        merger = ls.MergeToSingleStream(name="merger")
        dumper = ls.Dump(print_fun=collect_result)

        # Build pipeline: (source1 | source2) -> merger -> dumper
        sponge = (source1 | source2) * merger * dumper

        # Benchmark the pipeline
        start_time = time.perf_counter()
        sponge.start()
        sponge.join()
        end_time = time.perf_counter()

        duration = end_time - start_time

        return {
            "streams": 2,
            "items_per_stream": n_items_per_stream,
            "total_input_items": n_items_per_stream * 2,
            "items_output": len(results),
            "duration_seconds": duration,
            "throughput_items_per_sec": len(results) / duration if duration > 0 else 0,
            "merge_ratio": len(results) / (n_items_per_stream * 2) if n_items_per_stream > 0 else 0,
            "expected_merge_ratio": 0.5,  # Two streams merged into one
        }


def run_streaming_benchmarks():
    """Run all streaming pipeline benchmarks and print results."""
    print("=" * 70)
    print("LOGICSPONGE-CORE STREAMING PIPELINE BENCHMARKS")
    print("=" * 70)

    # Simple pipeline benchmark
    print("\n--- Simple Pipeline Benchmarks ---")

    print("Simple streaming pipeline (10K items):")
    result = StreamingPipelineBenchmarks.benchmark_simple_streaming_pipeline(10000)
    print(f"  Items input: {result['items_input']:,}")
    print(f"  Items output: {result['items_output']:,}")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")
    print(f"  Pipeline stages: {result['pipeline_stages']}")

    print("\nParallel pipeline (10K items):")
    result = StreamingPipelineBenchmarks.benchmark_parallel_pipeline(10000)
    print(f"  Items input: {result['items_input']:,}")
    print(f"  Even items: {result['even_items']:,}")
    print(f"  Odd items: {result['odd_items']:,}")
    print(f"  Total output: {result['total_output']:,}")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")
    print(f"  Parallel branches: {result['parallel_branches']}")

    # Complex pipeline benchmark
    print("\n--- Complex Pipeline Benchmarks ---")

    print("Complex processing pipeline (10K items):")
    result = StreamingPipelineBenchmarks.benchmark_complex_processing_pipeline(10000)
    print(f"  Items input: {result['items_input']:,}")
    print(f"  Items output: {result['items_output']:,}")
    print(f"  Processing efficiency: {result['processing_efficiency']:.1%}")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")
    print(f"  Pipeline stages: {result['pipeline_stages']}")

    print("\nStreaming with delay (1K items, 1ms delay):")
    result = StreamingPipelineBenchmarks.benchmark_streaming_with_delay(1000, 1)
    print(f"  Items processed: {result['items_output']:,}")
    print(f"  Delay per item: {result['delay_per_item_ms']}ms")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Expected minimum: {result['expected_min_duration']:.3f}s")
    print(f"  Overhead ratio: {result['overhead_ratio']:.2f}x")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")

    # Stream merging benchmark
    print("\n--- Stream Merging Benchmarks ---")

    print("Merge streams pipeline (5K items per stream):")
    result = StreamingPipelineBenchmarks.benchmark_merge_streams_pipeline(5000)
    print(f"  Input streams: {result['streams']}")
    print(f"  Items per stream: {result['items_per_stream']:,}")
    print(f"  Total input: {result['total_input_items']:,}")
    print(f"  Items output: {result['items_output']:,}")
    print(f"  Merge ratio: {result['merge_ratio']:.1%} (expected: {result['expected_merge_ratio']:.1%})")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")

    print("\n" + "=" * 70)
    print("Streaming pipeline benchmarks completed!")


if __name__ == "__main__":
    run_streaming_benchmarks()
