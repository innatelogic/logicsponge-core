"""Test direct output wiring without explicit graph usage."""

import logicsponge.core as ls
from logicsponge.core.source import IterableSource


class Double(ls.FunctionTerm):
    """Double the incoming value."""

    def f(self, di: ls.DataItem) -> ls.DataItem:
        """Return item with doubled value."""
        return ls.DataItem({**di, "val": di["val"] * 2})


class Collect(ls.FunctionTerm):
    """Collect incoming items for assertions."""

    def __init__(self) -> None:
        """Initialize collection store."""
        super().__init__()
        self.seen: list[ls.DataItem] = []

    def f(self, di: ls.DataItem) -> None:
        """Store incoming items."""
        self.seen.append(di)


def test_direct_connect_output_to() -> None:
    """Ensure extra edges can be added without exposing graph primitives."""
    src = IterableSource([1, 2, 3])
    double = Double()
    collector = Collect()

    # default sequential path
    pipeline = src * double
    # add an extra edge from source directly to collector
    src.connect_output_to(collector, input_name="src_direct")

    # start once; component traversal starts both
    pipeline.start()
    pipeline.join()

    assert [di["val"] for di in collector.seen] == [1, 2, 3]
