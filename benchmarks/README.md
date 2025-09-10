# Logicsponge-Core Benchmarks

This directory contains comprehensive benchmarks for the logicsponge-core data streaming library.

## Overview

The benchmark suite measures performance across three key areas:

1. **Core Components** - Basic building blocks (DataItem, LatencyQueue, etc.)
2. **Memory Usage** - Memory consumption patterns
3. **Throughput** - Data streaming and processing rates

## Quick Start

Run all benchmarks:
```bash
python benchmarks/run_benchmarks.py
```

Run specific benchmark suite:
```bash
python benchmarks/run_benchmarks.py --suite core
python benchmarks/run_benchmarks.py --suite memory  
python benchmarks/run_benchmarks.py --suite throughput
```

## Benchmark Suites

### Core Benchmarks (`benchmark_core.py`)

Measures performance of fundamental components:

- **DataItem Creation/Access** - Object creation and field access performance
- **LatencyQueue Operations** - Timing measurement queue performance
- **SharedQueue Operations** - Thread-safe queue append, access, and view creation
- **Concurrent Access** - Multi-threaded SharedQueue performance
- **Stream Processing** - FunctionTerm and FilterTerm processing rates

### Memory Benchmarks (`benchmark_memory.py`)

Analyzes memory usage patterns:

- **DataItem Memory Usage** - Memory per item and field growth impact
- **SharedQueue Memory** - Queue memory usage with varying item counts
- **Queue Views Memory** - Memory overhead of multiple queue views
- **LatencyQueue Memory** - Memory usage during operation

### Throughput Benchmarks (`benchmark_throughput.py`)

Measures data streaming performance:

- **Source Throughput** - IterableSource streaming rates
- **Processing Throughput** - FunctionTerm and FilterTerm processing rates
- **Concurrent Producers** - Multi-threaded data production performance
- **Producer-Consumer** - Pipeline processing efficiency

## Benchmark Components

### Key Components Tested

1. **DataItem** - Core data container
   - Creation performance
   - Field access speed
   - Memory usage per item

2. **SharedQueue** - Thread-safe queue implementation
   - Append operations
   - Random access
   - Concurrent access patterns
   - Memory efficiency

3. **LatencyQueue** - Performance measurement queue
   - Tic/toc operations
   - Average calculation
   - Memory usage

4. **Source Components**
   - IterableSource streaming
   - CSV source creation
   - Processing pipelines

5. **Stream Processing Terms**
   - FunctionTerm processing
   - FilterTerm filtering
   - Pipeline throughput

### Performance Metrics

- **Latency** - Time per operation (milliseconds)
- **Throughput** - Items processed per second
- **Memory Usage** - Peak and per-item memory consumption
- **Concurrency** - Multi-threaded performance characteristics

## Example Output

```
LOGICSPONGE-CORE BENCHMARKS
============================================================

--- Core Component Benchmarks ---
DataItem creation (10K items):
  Mean: 45.123ms, Min: 42.456ms, Max: 48.789ms

SharedQueue append (10K items):
  Mean: 23.456ms, Min: 21.123ms, Max: 25.789ms

--- Memory Usage ---
DataItem creation (10K items):
  Peak memory: 12.34 MB
  Per item: 1.234 KB

--- Throughput ---
IterableSource throughput (100K items):
  Throughput: 125,000 items/sec
  Duration: 0.800s
```

## Requirements

The benchmarks require the same dependencies as the main project:
- Python >= 3.11
- logicsponge-core dependencies (frozendict, readerwriterlock, etc.)

## Implementation Details

### BenchmarkRunner Class
- Configurable warmup and benchmark runs
- Statistical analysis (mean, min, max, std deviation)
- Consistent timing methodology

### Memory Measurement
- Uses `tracemalloc` for accurate memory tracking
- Forced garbage collection for clean measurements
- Per-item memory usage calculations

### Throughput Measurement  
- Thread-safe collectors for concurrent scenarios
- Realistic data generation
- Pipeline efficiency metrics

### Concurrent Testing
- Threading barriers for synchronized starts
- Lock-based coordination
- Realistic producer-consumer patterns

## Customization

Benchmark parameters can be adjusted by modifying the test functions:

```python
# Adjust item counts
core_bench.benchmark_data_item_creation(n_items=50000)

# Change concurrency levels
queue_bench.benchmark_concurrent_access(n_threads=8)

# Modify throughput test sizes
DataStreamThroughputBenchmarks.benchmark_iterable_source_throughput(n_items=1000000)
```

## Best Practices

1. **Consistent Environment** - Run benchmarks on the same system for comparison
2. **Warm JIT** - Benchmarks include warmup runs for accurate measurements  
3. **Multiple Runs** - Results are averaged over multiple runs for stability
4. **Resource Cleanup** - Memory benchmarks force garbage collection
5. **Realistic Workloads** - Tests use realistic data patterns and sizes

## Contributing

To add new benchmarks:

1. Create benchmark functions following the existing pattern
2. Use the `BenchmarkRunner` class for timing
3. Include memory measurements where relevant
4. Add throughput tests for streaming operations
5. Update this README with new benchmark descriptions