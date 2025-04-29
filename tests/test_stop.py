"""Test stopping."""

import logging

import logicsponge.core as ls

# logging
logger = logging.getLogger(__name__)


def test_run_dataitem() -> None:
    """Test run with stop."""

    class MySource(ls.SourceTerm):
        def run(self) -> None:
            for i in range(10):
                self.output(ls.DataItem({"say": i}))

        def exit(self) -> None:
            logger.info("MySource.exit")

    class MyF(ls.FunctionTerm):
        def f(self, di: ls.DataItem) -> None:
            if not self.state:
                self.state["say_expected"] = 0
            else:
                self.state["say_expected"] += 1

            logger.info(di)
            assert di["say"] == self.state["say_expected"]
            self.output(di)

        def exit(self) -> None:
            logger.info("MyF.exit")

    sponge = MySource() * MyF("a") * MyF("b")
    sponge.start()
    sponge.join()


if __name__ == "__main__":
    test_run_dataitem()
