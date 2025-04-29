"""Test queues."""

import pytest

from logicsponge.core.datastructures import SharedQueue, SharedQueueView


def test_empty() -> None:
    """Test empty queue."""
    queue: SharedQueue[int] = SharedQueue()

    assert len(queue) == 0

    with pytest.raises(IndexError) as _:
        queue[0]


def test_append_one() -> None:
    """Test adding."""
    queue: SharedQueue[int] = SharedQueue()
    queue.append(42)

    assert len(queue) == 1

    assert queue[0] == 42
    assert queue[-1] == 42

    with pytest.raises(IndexError) as _:
        queue[1]

    with pytest.raises(IndexError) as _:
        queue[-2]


def test_create_view() -> None:
    """Test creating a view."""
    queue: SharedQueue[int] = SharedQueue()
    queue.append(42)

    view: SharedQueueView[int] = queue.create_view()

    assert view is not None

    with pytest.raises(IndexError) as _:
        view[0]


def test_view_next() -> None:
    """Test views."""
    queue: SharedQueue[int] = SharedQueue()
    queue.append(42)
    queue.append(43)

    view: SharedQueueView[int] = queue.create_view()
    view.next()

    assert view[0] == 42
    assert view[-1] == 42

    with pytest.raises(IndexError) as _:
        view[1]

    with pytest.raises(IndexError) as _:
        view[-2]


def test_drop_one() -> None:
    """Test dropping one."""
    queue: SharedQueue[int] = SharedQueue()
    queue.append(42)
    queue.append(43)

    queue.drop_front(cnt=1)

    assert queue.to_list() == [43]


def test_drop_all() -> None:
    """Test dropping two."""
    queue: SharedQueue[int] = SharedQueue()
    queue.append(42)
    queue.append(43)

    queue.drop_front(cnt=2)

    assert queue.to_list() == []


def test_drop_none() -> None:
    """Test dropping none."""
    queue: SharedQueue[int] = SharedQueue()
    queue.append(42)
    queue.append(43)

    queue.drop_front(cnt=0)

    assert queue.to_list() == [42, 43]
