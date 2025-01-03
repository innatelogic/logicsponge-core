# type: ignore

import logging

import logicsponge.core as ls

# logging
logger = logging.getLogger(__name__)


def test_run_dataitem():
    class MySource(ls.SourceTerm):
        def run(self):
            for i in range(10):
                self.output(ls.DataItem({"say": i}))

        def exit(self):
            logger.info("MySource.exit")

    class MyF(ls.FunctionTerm):
        def f(self, di: ls.DataItem):
            if not self.state:
                self.state["say_expected"] = 0
            else:
                self.state["say_expected"] += 1

            logger.info(di)
            assert di["say"] == self.state["say_expected"]
            self.output(di)

        def exit(self):
            logger.info("MyF.exit")

    sponge = MySource() * MyF("a") * MyF("b")
    sponge.start()


if __name__ == "__main__":
    test_run_dataitem()
