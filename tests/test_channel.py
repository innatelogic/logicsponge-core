"""Test channel."""

from logicsponge.core import DataItem
from logicsponge.core_rs import make_channel


def test_send_one() -> None:
    """Test sending one element through the channel."""
    tx, rx = make_channel()

    tx.send(DataItem({"a": 42}))

    assert rx.recv() == DataItem({"a": 42})
