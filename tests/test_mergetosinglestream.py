"""Test merging streams."""

import logicsponge.core as ls


def test_nocombine() -> None:
    """Test not combining."""

    class MySource(ls.SourceTerm):
        def run(self) -> None:
            ds = ls.DataItem({"out": 0})
            self.output(ds)

    outputs = []
    sink = ls.Dump(print_fun=lambda di: outputs.append(di))
    sponge = (MySource("a") | MySource("b")) * ls.MergeToSingleStream() * sink
    sponge.start()
    sponge.join()

    expected_outputs = [ls.DataItem({"a": ls.DataItem({"out": 0}), "b": ls.DataItem({"out": 0})})]

    assert outputs == expected_outputs


def test_combine_overwrite() -> None:
    """Test combining."""

    class MySource(ls.SourceTerm):
        def run(self) -> None:
            ds = ls.DataItem({"out": 0})
            self.output(ds)

    outputs = []
    sink = ls.Dump(print_fun=lambda di: outputs.append(di))
    sponge = (MySource("a") | MySource("b")) * ls.MergeToSingleStream(combine=True) * sink
    sponge.start()
    sponge.join()

    expected_outputs = [ls.DataItem({"out": 0})]

    assert outputs == expected_outputs


def test_combine() -> None:
    """Test combining."""

    class MySource(ls.SourceTerm):
        def run(self) -> None:
            ds = ls.DataItem({self.name: 0})
            self.output(ds)

    outputs = []
    sink = ls.Dump(print_fun=lambda di: outputs.append(di))
    sponge = (MySource("a") | MySource("b")) * ls.MergeToSingleStream(combine=True) * sink
    sponge.start()
    sponge.join()

    expected_outputs = [ls.DataItem({"a": 0, "b": 0})]

    assert outputs == expected_outputs, outputs


def test_nocombine_flatten() -> None:
    """Test not combining with flatten."""

    class MySource(ls.SourceTerm):
        def run(self) -> None:
            ds = ls.DataItem({"out": 0})
            self.output(ds)

    outputs = []
    sink = ls.Dump(print_fun=lambda di: outputs.append(di))
    sponge = (MySource("a") | MySource("b")) * ls.MergeToSingleStream() * ls.Flatten() * sink
    sponge.start()
    sponge.join()

    expected_outputs = [ls.DataItem({"a.out": 0, "b.out": 0})]

    assert outputs == expected_outputs
