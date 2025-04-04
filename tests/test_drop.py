# type: ignore

import pytest

import logicsponge.core as ls


def test_datastream_forget_all():
    ds = ls.DataStream(owner=ls.Id())

    di = ls.DataItem({"hello": "world"})
    ds.add_row(di)
    assert di == ds[-1]

    ds.set_history_bound(ls.NumberBound(0))
    ds.clean_history()

    with pytest.raises(IndexError) as _:
        ds[0]

    with pytest.raises(IndexError) as _:
        ds[-1]


def test_datastreamview_forget_all():
    term = ls.Id()
    ds = ls.DataStream(owner=term)
    dsv = ls.DataStreamView(ds=ds, owner=term)

    di = ls.DataItem({"hello": "world"})
    ds.add_row(di)
    dsv.next()
    assert di == dsv[-1]

    ds.set_history_bound(ls.NumberBound(0))
    ds.clean_history()

    with pytest.raises(IndexError) as _:
        dsv[-1]


def test_datastream_keep_newest():
    ds = ls.DataStream(owner=ls.Id())
    ds.set_history_bound(ls.NumberBound(1))

    dis = [ls.DataItem({"hello": "world", "cnt": i}) for i in range(10)]
    for di in dis:
        ds.add_row(di)
        assert di == ds[-1]
        with pytest.raises(IndexError) as _:
            ds[-2]


def test_datastreamview_keep_newest():
    term = ls.Id()
    ds = ls.DataStream(owner=term)
    ds.set_history_bound(ls.NumberBound(1))
    dsv = ls.DataStreamView(ds=ds, owner=term)

    dis = [ls.DataItem({"hello": "world", "cnt": i}) for i in range(10)]
    for di in dis:
        ds.add_row(di)

    dsv.next()
    assert dis[0] == dsv[-1]
