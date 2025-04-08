# type: ignore


import logicsponge.core as ls


def test_getitem():
    di: ls.DataItem = ls.DataItem({"hello": "world"})
    assert di["hello"] == "world"
