"""Test joining."""

import logicsponge.core as ls


def test_source() -> None:
    """Test source."""

    class MySource(ls.SourceTerm):
        def run(self) -> None:
            for i in range(10):
                ds = ls.DataItem({"subject_id": i})
                self.output(ds)

    sponge = MySource()
    sponge.start()
    sponge.join()


def test_id() -> None:
    """Test Id."""

    class MySource(ls.SourceTerm):
        def run(self) -> None:
            for i in range(10):
                ds = ls.DataItem({"subject_id": i})
                self.output(ds)

    sponge = MySource() * ls.Id()
    sponge.start()
    sponge.join()


def test_parallel_id() -> None:
    """Test parallel Id."""

    class MySource(ls.SourceTerm):
        def run(self) -> None:
            for i in range(10):
                ds = ls.DataItem({"subject_id": i})
                self.output(ds)

    sponge = MySource() * (ls.Id("a") | ls.Id("b"))
    sponge.start()
    sponge.join()
