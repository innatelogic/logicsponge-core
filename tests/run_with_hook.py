# ruff: noqa: T201
"""Run a single example with a 5-second timeout. Report any uncaught exceptions during that time."""

import multiprocessing
import pathlib
import sys
import threading

import matplotlib as mpl

# === Constants for communication ===
SUCCESS = "success"
THREAD_FAILED = "thread_failed"
MAIN_FAILED = "main_failed"


def custom_excepthook(args: threading.ExceptHookArgs) -> None:
    """Exception hook that notes the fact that a thread raised an exception."""
    if args.thread:
        print(f"❌ Thread {args.thread.name} crashed: {args.exc_value}")
    else:
        print(f"❌ A thread crashed: {args.exc_value}")
    # Notify main process through the queue (via global)
    if thread_crash_queue:
        thread_crash_queue.put(THREAD_FAILED)


# This will be redefined inside the subprocess, so it's global here for scope only
thread_crash_queue = None


def run_script(script: str, queue: multiprocessing.Queue) -> None:
    """Run the script of the given name and report any errors via the given queue."""
    global thread_crash_queue  # noqa: PLW0603
    thread_crash_queue = queue

    threading.excepthook = custom_excepthook

    def run() -> None:
        try:
            mpl.use("Agg")
            with pathlib.Path(script).open(encoding=None) as f:
                code = compile(f.read(), script, "exec")
                exec(code, {"__name__": "__main__"})
            # Only put success if no crash was already reported
            if queue.empty():
                queue.put(SUCCESS)
        except Exception as e:  # noqa: BLE001
            print(f"❌ Top-level exception: {e}")
            queue.put(MAIN_FAILED)

    runner = threading.Thread(target=run, name="main_runner")
    runner.start()
    runner.join(timeout=5)

    if runner.is_alive():
        print("⏱️ Timeout inside subprocess")
        return  # Parent process handles timeout — no need to exit here


def run_with_timeout(script: str) -> None:
    """Run the script of the given name for the given timeout and report exceptions."""
    queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=run_script, args=(script, queue))
    process.start()
    process.join(timeout=5)

    result = None
    if not queue.empty():
        result = queue.get()

    if process.is_alive():
        print("⏱️ Subprocess timed out — force killing")
        process.terminate()
        process.join()
        # If we received any error before killing
        if result in {THREAD_FAILED, MAIN_FAILED}:
            sys.exit(1)
        sys.exit(0)  # Clean timeout

    # Process completed in time — check result
    if result == THREAD_FAILED:
        print("❗ A thread crashed")
        sys.exit(1)
    elif result == MAIN_FAILED:
        print("❌ Script raised a top-level exception")
        sys.exit(1)
    elif result == SUCCESS:
        print("✅ Script ran cleanly")
        sys.exit(0)
    else:
        print("❓ No result received — unexpected state")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_with_hook.py <script>")
        sys.exit(1)

    script = sys.argv[1]
    print(f"▶ Running {script} with 5s timeout...")
    run_with_timeout(script)
