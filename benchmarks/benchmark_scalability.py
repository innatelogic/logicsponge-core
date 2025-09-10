"""Scalability benchmarks for logicsponge-core with many terms and threads."""

import time
import threading
import psutil
import os
from typing import Any

import logicsponge.core as ls
from logicsponge.core.source import IterableSource


class ScalabilityBenchmarks:
    """Benchmarks to test system behavior with many terms and threads."""

    @staticmethod
    def benchmark_many_sequential_terms(n_terms: int = 100, n_items: int = 1000) -> dict[str, Any]:
        """Benchmark long sequential pipeline with many terms."""

        # Create input data
        inputs = [{"id": i, "value": i} for i in range(n_items)]

        # Create results collector
        results = []

        def collect_result(item):
            results.append(item)

        # Create source
        source = IterableSource(inputs, name="scalability_source")

        # Create many processing terms in sequence
        class IncrementTerm(ls.FunctionTerm):
            def __init__(self, increment: int):
                super().__init__(name=f"increment_{increment}")
                self.increment = increment

            def f(self, data: ls.DataItem) -> ls.DataItem:
                return ls.DataItem({**data, "value": data["value"] + self.increment})

        # Build long sequential pipeline
        pipeline = source
        for i in range(n_terms):
            pipeline = pipeline * IncrementTerm(increment=1)

        # Add final dump
        pipeline = pipeline * ls.Dump(print_fun=collect_result)

        # Monitor system resources
        process = psutil.Process(os.getpid())
        initial_threads = process.num_threads()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Benchmark the pipeline
        start_time = time.perf_counter()
        pipeline.start()

        # Check resources during execution
        peak_threads = process.num_threads()
        peak_memory = process.memory_info().rss / 1024 / 1024  # MB

        pipeline.join()
        end_time = time.perf_counter()

        # Final resource check
        final_threads = process.num_threads()
        final_memory = process.memory_info().rss / 1024 / 1024  # MB

        duration = end_time - start_time

        # Calculate expected final value (each item incremented n_terms times)
        expected_final_value = n_items - 1 + n_terms  # Last item (n_items-1) + n_terms increments
        actual_final_value = results[-1]["value"] if results else 0

        return {
            "n_terms": n_terms,
            "n_items": n_items,
            "items_output": len(results),
            "duration_seconds": duration,
            "throughput_items_per_sec": len(results) / duration if duration > 0 else 0,
            "expected_final_value": expected_final_value,
            "actual_final_value": actual_final_value,
            "computation_correct": actual_final_value == expected_final_value,
            "initial_threads": initial_threads,
            "peak_threads": peak_threads,
            "final_threads": final_threads,
            "thread_overhead": peak_threads - initial_threads,
            "initial_memory_mb": initial_memory,
            "peak_memory_mb": peak_memory,
            "final_memory_mb": final_memory,
            "memory_overhead_mb": peak_memory - initial_memory,
        }

    @staticmethod
    def benchmark_many_parallel_terms(n_terms: int = 50, n_items: int = 1000) -> dict[str, Any]:
        """Benchmark wide parallel pipeline with many terms."""

        # Create input data
        inputs = [{"id": i, "value": i} for i in range(n_items)]

        # Create results collectors (one per parallel branch)
        results = []
        results_lock = threading.Lock()

        def collect_result(item):
            with results_lock:
                results.append(item)

        # Create source
        source = IterableSource(inputs, name="parallel_source")

        # Create many parallel processing terms
        class MultiplierTerm(ls.FunctionTerm):
            def __init__(self, multiplier: int):
                super().__init__(name=f"mult_{multiplier}")
                self.multiplier = multiplier

            def f(self, data: ls.DataItem) -> ls.DataItem:
                return ls.DataItem({**data, "value": data["value"] * self.multiplier, "multiplier": self.multiplier})

        # Create parallel branches
        parallel_terms = []
        for i in range(n_terms):
            # Each branch: multiplier -> dump
            branch = MultiplierTerm(multiplier=i + 1) * ls.Dump(print_fun=collect_result, name=f"dump_{i}")
            parallel_terms.append(branch)

        # Build parallel pipeline: source -> (term1 | term2 | ... | termN)
        if len(parallel_terms) == 1:
            pipeline = source * parallel_terms[0]
        else:
            parallel_section = parallel_terms[0]
            for term in parallel_terms[1:]:
                parallel_section = parallel_section | term
            pipeline = source * parallel_section

        # Monitor system resources
        process = psutil.Process(os.getpid())
        initial_threads = process.num_threads()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Benchmark the pipeline
        start_time = time.perf_counter()
        pipeline.start()

        # Check resources during execution
        peak_threads = process.num_threads()
        peak_memory = process.memory_info().rss / 1024 / 1024  # MB

        pipeline.join()
        end_time = time.perf_counter()

        # Final resource check
        final_threads = process.num_threads()
        final_memory = process.memory_info().rss / 1024 / 1024  # MB

        duration = end_time - start_time

        # Each input item should produce n_terms output items (one per branch)
        expected_output_count = n_items * n_terms

        return {
            "n_terms": n_terms,
            "n_items": n_items,
            "expected_output": expected_output_count,
            "actual_output": len(results),
            "output_ratio": len(results) / expected_output_count if expected_output_count > 0 else 0,
            "duration_seconds": duration,
            "throughput_items_per_sec": len(results) / duration if duration > 0 else 0,
            "initial_threads": initial_threads,
            "peak_threads": peak_threads,
            "final_threads": final_threads,
            "thread_overhead": peak_threads - initial_threads,
            "threads_per_term": (peak_threads - initial_threads) / n_terms if n_terms > 0 else 0,
            "initial_memory_mb": initial_memory,
            "peak_memory_mb": peak_memory,
            "final_memory_mb": final_memory,
            "memory_overhead_mb": peak_memory - initial_memory,
            "memory_per_term_mb": (peak_memory - initial_memory) / n_terms if n_terms > 0 else 0,
        }

    @staticmethod
    def benchmark_scaling_thread_count(max_terms: int = 200, step: int = 20, n_items: int = 500) -> dict[str, Any]:
        """Benchmark how performance scales with increasing term count."""

        scaling_results = []

        for n_terms in range(step, max_terms + 1, step):
            print(f"  Testing with {n_terms} terms...")

            # Run parallel benchmark for this term count
            result = ScalabilityBenchmarks.benchmark_many_parallel_terms(n_terms, n_items)

            scaling_results.append(
                {
                    "n_terms": n_terms,
                    "throughput": result["throughput_items_per_sec"],
                    "thread_overhead": result["thread_overhead"],
                    "threads_per_term": result["threads_per_term"],
                    "memory_overhead_mb": result["memory_overhead_mb"],
                    "memory_per_term_mb": result["memory_per_term_mb"],
                    "duration": result["duration_seconds"],
                    "output_ratio": result["output_ratio"],
                }
            )

        # Find performance characteristics
        best_throughput = max(scaling_results, key=lambda x: x["throughput"])
        worst_throughput = min(scaling_results, key=lambda x: x["throughput"])

        return {
            "scaling_data": scaling_results,
            "max_terms_tested": max_terms,
            "step_size": step,
            "best_throughput": best_throughput,
            "worst_throughput": worst_throughput,
            "throughput_degradation": (best_throughput["throughput"] - worst_throughput["throughput"])
            / best_throughput["throughput"]
            * 100,
            "max_thread_overhead": max(r["thread_overhead"] for r in scaling_results),
            "max_memory_overhead_mb": max(r["memory_overhead_mb"] for r in scaling_results),
        }

    @staticmethod
    def benchmark_thread_cleanup(n_terms: int = 100, n_runs: int = 5) -> dict[str, Any]:
        """Test if threads are properly cleaned up after multiple pipeline runs."""

        cleanup_results = []
        process = psutil.Process(os.getpid())

        initial_threads = process.num_threads()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        for run in range(n_runs):
            print(f"  Run {run + 1}/{n_runs}...")

            # Create a pipeline with many terms
            inputs = [{"id": i, "value": i} for i in range(100)]
            results = []

            source = IterableSource(inputs, name=f"cleanup_source_{run}")

            class ProcessorTerm(ls.FunctionTerm):
                def __init__(self, term_id: int):
                    super().__init__(name=f"processor_{term_id}")
                    self.term_id = term_id

                def f(self, data: ls.DataItem) -> ls.DataItem:
                    return ls.DataItem({**data, f"processed_by_{self.term_id}": True})

            # Build sequential pipeline
            pipeline = source
            for i in range(n_terms):
                pipeline = pipeline * ProcessorTerm(i)
            pipeline = pipeline * ls.Dump(print_fun=lambda x: results.append(x))

            # Run pipeline
            start_time = time.perf_counter()
            pipeline.start()
            pipeline.join()
            end_time = time.perf_counter()

            # Check resource usage after this run
            threads_after = process.num_threads()
            memory_after = process.memory_info().rss / 1024 / 1024  # MB

            cleanup_results.append(
                {
                    "run": run + 1,
                    "duration": end_time - start_time,
                    "items_processed": len(results),
                    "threads_after": threads_after,
                    "memory_after_mb": memory_after,
                    "thread_growth": threads_after - initial_threads,
                    "memory_growth_mb": memory_after - initial_memory,
                }
            )

            # Brief pause between runs
            time.sleep(0.1)

        final_threads = process.num_threads()
        final_memory = process.memory_info().rss / 1024 / 1024  # MB

        return {
            "n_runs": n_runs,
            "n_terms_per_run": n_terms,
            "initial_threads": initial_threads,
            "final_threads": final_threads,
            "initial_memory_mb": initial_memory,
            "final_memory_mb": final_memory,
            "total_thread_growth": final_threads - initial_threads,
            "total_memory_growth_mb": final_memory - initial_memory,
            "run_details": cleanup_results,
            "threads_properly_cleaned": final_threads <= initial_threads + 5,  # Allow small overhead
            "memory_leak_detected": final_memory > initial_memory + 50,  # Flag if >50MB growth
        }


def run_scalability_benchmarks():
    """Run all scalability benchmarks and print results."""
    print("=" * 70)
    print("LOGICSPONGE-CORE SCALABILITY BENCHMARKS")
    print("=" * 70)

    # Sequential terms benchmark
    print("\n--- Sequential Pipeline Scaling ---")

    print("Sequential pipeline with 100 terms (1K items):")
    result = ScalabilityBenchmarks.benchmark_many_sequential_terms(100, 1000)
    print(f"  Terms: {result['n_terms']}")
    print(f"  Items processed: {result['items_output']:,}")
    print(f"  Computation correct: {result['computation_correct']}")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")
    print(f"  Thread overhead: {result['thread_overhead']} threads")
    print(f"  Memory overhead: {result['memory_overhead_mb']:.1f} MB")

    # Parallel terms benchmark
    print("\n--- Parallel Pipeline Scaling ---")

    print("Parallel pipeline with 50 terms (1K items):")
    result = ScalabilityBenchmarks.benchmark_many_parallel_terms(50, 1000)
    print(f"  Terms: {result['n_terms']}")
    print(f"  Expected output: {result['expected_output']:,}")
    print(f"  Actual output: {result['actual_output']:,}")
    print(f"  Output ratio: {result['output_ratio']:.1%}")
    print(f"  Duration: {result['duration_seconds']:.3f}s")
    print(f"  Throughput: {result['throughput_items_per_sec']:,.0f} items/sec")
    print(f"  Thread overhead: {result['thread_overhead']} threads")
    print(f"  Threads per term: {result['threads_per_term']:.1f}")
    print(f"  Memory overhead: {result['memory_overhead_mb']:.1f} MB")
    print(f"  Memory per term: {result['memory_per_term_mb']:.2f} MB")

    # Scaling analysis
    print("\n--- Threading Scalability Analysis ---")

    print("Testing scalability from 20 to 200 terms...")
    result = ScalabilityBenchmarks.benchmark_scaling_thread_count(200, 20, 500)
    print(f"  Max terms tested: {result['max_terms_tested']}")
    print(
        f"  Best throughput: {result['best_throughput']['throughput']:,.0f} items/sec ({result['best_throughput']['n_terms']} terms)"
    )
    print(
        f"  Worst throughput: {result['worst_throughput']['throughput']:,.0f} items/sec ({result['worst_throughput']['n_terms']} terms)"
    )
    print(f"  Throughput degradation: {result['throughput_degradation']:.1f}%")
    print(f"  Max thread overhead: {result['max_thread_overhead']} threads")
    print(f"  Max memory overhead: {result['max_memory_overhead_mb']:.1f} MB")

    # Thread cleanup test
    print("\n--- Thread Cleanup Analysis ---")

    print("Testing thread cleanup over 5 runs with 100 terms each...")
    result = ScalabilityBenchmarks.benchmark_thread_cleanup(100, 5)
    print(f"  Initial threads: {result['initial_threads']}")
    print(f"  Final threads: {result['final_threads']}")
    print(f"  Total thread growth: {result['total_thread_growth']}")
    print(f"  Threads properly cleaned: {result['threads_properly_cleaned']}")
    print(f"  Initial memory: {result['initial_memory_mb']:.1f} MB")
    print(f"  Final memory: {result['final_memory_mb']:.1f} MB")
    print(f"  Total memory growth: {result['total_memory_growth_mb']:.1f} MB")
    print(f"  Memory leak detected: {result['memory_leak_detected']}")

    print("\n" + "=" * 70)
    print("Scalability benchmarks completed!")


if __name__ == "__main__":
    run_scalability_benchmarks()
