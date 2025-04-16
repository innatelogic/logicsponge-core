import logging  # noqa: D100

import logicsponge.core as ls
from logicsponge.core.source import CSVStreamer

# logging
logger = logging.getLogger(__name__)


def test_streaming_csv() -> None:
    """Test csv streaming."""
    sponge = CSVStreamer(file_path="test.csv") * ls.Print()
    sponge.start()


if __name__ == "__main__":
    test_streaming_csv()
