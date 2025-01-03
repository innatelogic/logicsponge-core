import logging

import logicsponge.core as ls
from logicsponge.core.source import CSVStreamer

# logging
logger = logging.getLogger(__name__)


def test_streaming_csv():
    sponge = CSVStreamer(file_path="test.csv") * ls.Print()
    sponge.start()


if __name__ == "__main__":
    test_streaming_csv()
