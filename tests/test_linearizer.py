"""Test Linearizer."""

import logicsponge.core as ls
from logicsponge.core.source import IterableSource


def test_linearize_three() -> None:
    """Linearize three DataStreams."""
    inputs = {"a": ["A1", "A2", "A3"], "b": ["B1", "B2", "B3"], "c": ["C1", "C2", "C3"]}

    expected_outputs: list[ls.DataItem] = [
        ls.DataItem({"name": name, "data": ls.DataItem({"val": item})}) for name in inputs for item in inputs[name]
    ]
    outputs = []

    sponge = (
        (
            IterableSource(name="a", iterable=inputs["a"])
            | IterableSource(name="b", iterable=inputs["b"])
            | IterableSource(name="c", iterable=inputs["c"])
        )
        * ls.Linearizer()
        * ls.Dump(print_fun=outputs.append)
    )

    sponge.start()
    sponge.join()

    assert set(outputs) == set(expected_outputs)
