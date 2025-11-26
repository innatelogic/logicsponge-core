"""Test joining."""

from collections.abc import Iterator

import logicsponge.core as ls


def test_source() -> None:
    """Test source."""

    class MySource(ls.SourceTerm):
        def generate(self) -> Iterator[ls.DataItem]:
            for i in range(10):
                yield ls.DataItem({"subject_id": i})

    sponge = MySource()
    sponge.start()
    sponge.join()


def test_id() -> None:
    """Test Id."""

    class MySource(ls.SourceTerm):
        def generate(self) -> Iterator[ls.DataItem]:
            for i in range(10):
                yield ls.DataItem({"subject_id": i})

    sponge = MySource() * ls.Id()
    sponge.start()
    sponge.join()


def test_parallel_id() -> None:
    """Test parallel Id."""

    class MySource(ls.SourceTerm):
        def generate(self) -> Iterator[ls.DataItem]:
            for i in range(10):
                yield ls.DataItem({"subject_id": i})

    sponge = MySource() * (ls.Id("a") | ls.Id("b"))
    sponge.start()
    sponge.join()
