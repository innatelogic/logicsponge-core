"""Smoke test for the dataflow backend."""

import logicsponge.core as ls
from logicsponge.core.source import IterableSource


class Echo(ls.FunctionTerm):
    """Echo term for roundtrip testing."""

    def f(self, di: ls.DataItem) -> ls.DataItem:
        """Return the item unchanged."""
        return di


def test_flow_echo_roundtrip() -> None:
    """Ensure dataflow-backed graph runs end-to-end."""
    g = ls.TermGraph()
    src = IterableSource([1, 2, 3])
    echo = Echo()
    g.connect(src, echo, "in")

    results = ls.run_flow_graph(g)
    assert [dict(r) for r in results] == [{"val": 1}, {"val": 2}, {"val": 3}]
