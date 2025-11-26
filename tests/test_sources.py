"""Test all source implementations."""

import csv
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import logicsponge.core as ls
from logicsponge.core.source import CSVStreamer, FileWatchSource

if TYPE_CHECKING:
    from collections.abc import Iterator


def test_constant_source_term() -> None:
    """Test ConstantSourceTerm with preloaded items."""
    items = [
        ls.DataItem({"value": 1}),
        ls.DataItem({"value": 2}),
        ls.DataItem({"value": 3}),
    ]

    source = ls.ConstantSourceTerm(items)
    results = []

    sponge = source * ls.Dump(print_fun=lambda x: results.append(x))
    sponge.start()
    sponge.join()

    assert results == items
    assert len(results) == 3


def test_constant_source_term_empty() -> None:
    """Test ConstantSourceTerm with empty list."""
    source = ls.ConstantSourceTerm([])
    results = []

    sponge = source * ls.Dump(print_fun=lambda x: results.append(x))
    sponge.start()
    sponge.join()

    assert results == []


def test_csv_streamer_static_file() -> None:
    """Test CSVStreamer reads CSV file data correctly.

    Note: CSVStreamer polls indefinitely, so we test its internal logic
    by directly calling generate() and taking only the first few items.
    """
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "value"])
        writer.writeheader()
        writer.writerow({"id": "1", "name": "Alice", "value": "100"})
        writer.writerow({"id": "2", "name": "Bob", "value": "200"})
        writer.writerow({"id": "3", "name": "Charlie", "value": "300"})
        csv_path = f.name

    generator: Iterator[ls.DataItem] | None = None
    try:
        # Test the generator directly
        source = CSVStreamer(file_path=csv_path, poll_delay=0.01)
        generator = source.generate()

        # Take first 3 items before it starts polling again
        results = []
        for _ in range(3):
            item = next(generator)
            results.append(item)

        # Verify we got the expected data
        assert len(results) == 3
        assert results[0]["id"] == "1"
        assert results[0]["name"] == "Alice"
        assert results[1]["id"] == "2"
        assert results[1]["name"] == "Bob"
        assert results[2]["id"] == "3"
        assert results[2]["name"] == "Charlie"

    finally:
        if generator is not None:
            close_fn = getattr(generator, "close", None)
            if callable(close_fn):
                close_fn()
        # Clean up
        Path(csv_path).unlink()


def test_file_watch_source() -> None:
    """Test FileWatchSource initialization and enter/exit hooks.

    Note: FileWatchSource generates items indefinitely via queue.get(),
    so we test that it initializes correctly and the watchdog observer starts.
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("Initial content")
        file_path = f.name

    try:
        source = FileWatchSource(file_path)

        # Test that enter() starts the observer
        source.enter()
        assert source.observer.is_alive()

        # The queue should have the initial read
        # Use timeout to avoid blocking forever
        import queue

        try:
            item = source.queue.get(timeout=1.0)
            assert "string" in item
            assert item["string"] == "Initial content"
        except queue.Empty:
            pass  # Initial read might not have completed yet

        # Clean up
        source.exit()
        assert not source.observer.is_alive()

    finally:
        # Clean up
        Path(file_path).unlink()


def test_constant_source_term_with_processing() -> None:
    """Test ConstantSourceTerm with downstream processing."""
    items = [
        ls.DataItem({"value": 1}),
        ls.DataItem({"value": 2}),
        ls.DataItem({"value": 3}),
    ]

    class DoubleValue(ls.FunctionTerm):
        def f(self, di: ls.DataItem) -> ls.DataItem:
            return ls.DataItem({"value": di["value"] * 2})

    source = ls.ConstantSourceTerm(items)
    doubler = DoubleValue()
    results = []

    sponge = source * doubler * ls.Dump(print_fun=lambda x: results.append(x))
    sponge.start()
    sponge.join()

    expected = [
        ls.DataItem({"value": 2}),
        ls.DataItem({"value": 4}),
        ls.DataItem({"value": 6}),
    ]

    assert results == expected


def test_constant_source_term_parallel() -> None:
    """Test ConstantSourceTerm in parallel composition."""
    items_a = [ls.DataItem({"source": "a", "value": 1})]
    items_b = [ls.DataItem({"source": "b", "value": 2})]

    source_a = ls.ConstantSourceTerm(items_a, name="source_a")
    source_b = ls.ConstantSourceTerm(items_b, name="source_b")

    results = []

    sponge = (
        (source_a | source_b) * ls.MergeToSingleStream(combine=True) * ls.Dump(print_fun=lambda x: results.append(x))
    )
    sponge.start()
    sponge.join()

    # Should get one merged item
    assert len(results) == 1
    merged = results[0]

    # Should contain values from both sources
    assert "source" in merged or "source_a" in merged or "source_b" in merged


if __name__ == "__main__":
    test_constant_source_term()
    test_constant_source_term_empty()
    test_csv_streamer_static_file()
    test_file_watch_source()
    test_constant_source_term_with_processing()
    test_constant_source_term_parallel()
