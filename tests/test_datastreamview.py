# type: ignore

import pytest

import logicsponge.core as ls


def test_datastreamview_no_elements():
    term = ls.Id()
    ds = ls.DataStream(owner=term)
    dsv = ls.DataStreamView(ds=ds, owner=term)

    with pytest.raises(IndexError) as _:
        dsv[0]

    with pytest.raises(IndexError) as _:
        dsv[-1]


def test_datastreamview_one_element():
    term = ls.Id()
    ds = ls.DataStream(owner=term)
    dsv = ls.DataStreamView(ds=ds, owner=term)

    di0 = ls.DataItem({"hello": "world"})
    ds.add_row(di0)
    dsv.next()

    assert di0 == dsv[0]
    assert di0 == dsv[-1]

    with pytest.raises(IndexError) as _:
        ds[1]

    with pytest.raises(IndexError) as _:
        ds[-2]


def test_datastreamview_two_elements():
    term = ls.Id()
    ds = ls.DataStream(owner=term)
    dsv = ls.DataStreamView(ds=ds, owner=term)

    di0 = ls.DataItem({"hello": "world"})
    di1 = ls.DataItem({"hello": "earth"})
    ds.add_row(di0)
    ds.add_row(di1)
    dsv.next()
    dsv.next()

    assert di0 == dsv[0]
    assert di1 == dsv[-1]
    assert di1 == dsv[1]
    assert di0 == dsv[-2]

    with pytest.raises(IndexError) as _:
        ds[2]

    with pytest.raises(IndexError) as _:
        ds[-3]
