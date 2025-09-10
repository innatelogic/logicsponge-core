#!/usr/bin/env python3
"""Quick benchmark runner for logicsponge-core."""

import sys
from pathlib import Path

# Add benchmarks directory to path
benchmarks_dir = Path(__file__).parent / "benchmarks"
sys.path.insert(0, str(benchmarks_dir))

from run_benchmarks import main

if __name__ == "__main__":
    main()
