"""Testing data items."""

from typing import TypedDict

import logicsponge.core as ls


def test_getitem() -> None:
    """Test creation of a DataItem."""
    di: ls.DataItem = ls.DataItem({"hello": "world"})
    assert di["hello"] == "world"


def test_typing() -> None:
    """Test typing of a DataItem."""

    class Msg(TypedDict):
        message: str
        time: float

    # this should show a type error, currently it does not
    di: ls.DataItem[Msg] = ls.DataItem({"not-message": "world", "time": 10})
    assert di["not-message"] == "world"
