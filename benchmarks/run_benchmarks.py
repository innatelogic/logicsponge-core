"""Main benchmark runner for logicsponge-core."""

import sys
import argparse
import time
from pathlib import Path

# Add the project root to the path so we can import our modules
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from benchmarks.benchmark_core import run_all_benchmarks as run_core_benchmarks
from benchmarks.benchmark_memory import run_memory_benchmarks
from benchmarks.benchmark_throughput import run_throughput_benchmarks
from benchmarks.benchmark_streaming import run_streaming_benchmarks
from benchmarks.benchmark_scalability import run_scalability_benchmarks


def run_all_benchmarks():
    """Run all benchmark suites."""
    print("🚀 Starting comprehensive logicsponge-core benchmarks...")
    print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Run core benchmarks
    print("\n🔧 CORE COMPONENT BENCHMARKS")
    run_core_benchmarks()

    # Run memory benchmarks
    print("\n🧠 MEMORY USAGE BENCHMARKS")
    run_memory_benchmarks()

    # Run throughput benchmarks
    print("\n⚡ THROUGHPUT BENCHMARKS")
    run_throughput_benchmarks()

    # Run streaming pipeline benchmarks
    print("\n🌊 STREAMING PIPELINE BENCHMARKS")
    run_streaming_benchmarks()

    # Run scalability benchmarks
    print("\n📈 SCALABILITY BENCHMARKS")
    run_scalability_benchmarks()

    print("\n" + "=" * 80)
    print("✅ All benchmarks completed successfully!")
    print(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """Main entry point for benchmark runner."""
    parser = argparse.ArgumentParser(
        description="Run benchmarks for logicsponge-core data streaming library",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available benchmark suites:
  core        - Core component performance (DataItem, LatencyQueue, etc.)
  memory      - Memory usage analysis
  throughput  - Streaming throughput measurements
  streaming   - Real streaming pipeline benchmarks (start/join)
  scalability - Threading scalability with many terms
  all         - Run all benchmark suites (default)

Examples:
  python run_benchmarks.py                 # Run all benchmarks
  python run_benchmarks.py --suite core    # Run only core benchmarks
  python run_benchmarks.py --suite memory  # Run only memory benchmarks
        """,
    )

    parser.add_argument(
        "--suite",
        choices=["core", "memory", "throughput", "streaming", "scalability", "all"],
        default="all",
        help="Which benchmark suite to run (default: all)",
    )

    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")

    args = parser.parse_args()

    if not args.quiet:
        print("logicsponge-core Benchmark Suite")
        print("=" * 40)
        print(f"Running suite: {args.suite}")
        print(f"Python version: {sys.version}")
        print()

    try:
        if args.suite == "core":
            run_core_benchmarks()
        elif args.suite == "memory":
            run_memory_benchmarks()
        elif args.suite == "throughput":
            run_throughput_benchmarks()
        elif args.suite == "streaming":
            run_streaming_benchmarks()
        elif args.suite == "scalability":
            run_scalability_benchmarks()
        else:  # "all"
            run_all_benchmarks()

    except KeyboardInterrupt:
        print("\n⏹️  Benchmarks interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error running benchmarks: {e}")
        if not args.quiet:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
