"""Run the basic examples from logicsponge-examples for 5 seconds."""

import pathlib
import subprocess
import sys

scripts = sorted(pathlib.Path().glob("logicsponge-examples-main/basic/*.py"))

any_fail = False

for script in scripts:
    result = subprocess.run(
        [sys.executable, "tests/run_with_hook.py", script],
        stdout=sys.stdout,
        stderr=sys.stderr,
        check=False,
    )
    if result.returncode != 0:
        any_fail = True

if any_fail:
    sys.exit(1)
else:
    sys.exit(0)
