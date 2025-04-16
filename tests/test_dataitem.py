"""Testing data items."""

import logicsponge.core as ls


def test_getitem() -> None:
    """Test creation of a DataItem."""
    di: ls.DataItem = ls.DataItem({"hello": "world"})
    assert di["hello"] == "world"
