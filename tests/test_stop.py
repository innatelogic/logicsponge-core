"""Test stopping."""

import logging
from collections.abc import Iterator

import logicsponge.core as ls

# logging
logger = logging.getLogger(__name__)


def test_run_dataitem() -> None:
    """Test run with stop."""

    class MySource(ls.SourceTerm):
        def generate(self) -> Iterator[ls.DataItem]:
            for i in range(10):
                yield ls.DataItem({"say": i})

        def exit(self) -> None:
            logger.info("MySource.exit")

    class MyF(ls.StatefulFunctionTerm):
        def f(self, di: ls.DataItem) -> ls.DataItem:
            if not self.state:
                self.state["say_expected"] = 0
            else:
                self.state["say_expected"] += 1

            logger.info(di)
            assert di["say"] == self.state["say_expected"]
            return di

        def exit(self) -> None:
            logger.info("MyF.exit")

    sponge = MySource() * MyF("a") * MyF("b")
    sponge.start()
    sponge.join()


if __name__ == "__main__":
    test_run_dataitem()
