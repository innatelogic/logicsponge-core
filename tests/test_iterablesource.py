"""Test the IterableSource."""

import logicsponge.core as ls
from logicsponge.core.source import IterableSource


def test_empty() -> None:
    """Test empty source."""
    source: IterableSource = IterableSource()
    source.start()
    source.join()


def test_list_int() -> None:
    """Test iteration over list."""
    inputs: list = [1, 2, 3]
    expected_outputs: list[ls.DataItem] = [ls.DataItem({"val": item}) for item in inputs]

    source: IterableSource = IterableSource(inputs)
    res: list = []

    sponge = source * ls.Dump(print_fun=lambda x: res.append(x))
    sponge.start()
    sponge.join()

    assert res == expected_outputs


def test_list_str() -> None:
    """Test iteration over list."""
    inputs: list = ["a", "b", "c"]
    expected_outputs: list[ls.DataItem] = [ls.DataItem({"val": item}) for item in inputs]

    source: IterableSource = IterableSource(inputs)
    res: list = []

    sponge = source * ls.Dump(print_fun=lambda x: res.append(x))
    sponge.start()
    sponge.join()

    assert res == expected_outputs


def test_list_dict() -> None:
    """Test iteration over list."""
    inputs: list = [{"x": "a"}, {"x": "a"}, {"x": 1}]
    expected_outputs: list[ls.DataItem] = [ls.DataItem(item) for item in inputs]

    source: IterableSource = IterableSource(inputs)
    res: list = []

    sponge = source * ls.Dump(print_fun=lambda x: res.append(x))
    sponge.start()
    sponge.join()

    assert res == expected_outputs


def test_range() -> None:
    """Test iteration over range."""
    inputs: range = range(10)
    expected_outputs: list[ls.DataItem] = [ls.DataItem({"val": item}) for item in inputs]

    source: IterableSource = IterableSource(inputs)
    res: list = []

    sponge = source * ls.Dump(print_fun=lambda x: res.append(x))
    sponge.start()
    sponge.join()

    assert res == expected_outputs


if __name__ == "__main__":
    test_list_int()
