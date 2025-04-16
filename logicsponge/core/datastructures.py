"""Contains data structures for internal use in logicsponge-core."""

from threading import Condition
from typing import Generic, TypeVar

from readerwriterlock import rwlock

T = TypeVar("T")


class SharedQueueNode(Generic[T]):
    """Node in the SharedQueue."""

    value: T
    next: "SharedQueueNode | None"
    prev: "SharedQueueNode | None"

    def __init__(self, value: T) -> None:
        """Create a SharedQueueNode object."""
        self.value = value
        self.next = None
        self.prev = None


class SharedQueue(Generic[T]):
    """SharedQueue."""

    _head: SharedQueueNode[T] | None
    _tail: SharedQueueNode[T] | None

    _global_lock: rwlock.RWLockFair
    _new_data: Condition

    _next_cid: int
    _cursors: dict[int, SharedQueueNode[T] | None]

    def __init__(self) -> None:
        """Create a SharedQueue object."""
        self._head = None
        self._tail = None

        self._global_lock = rwlock.RWLockFair()
        self._new_data = Condition()

        self._next_cid = 0
        self._cursors = {}

    def __len__(self) -> int:
        """Return the length."""
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            count = 0
            node = self._head
            while node:
                count += 1
                node = node.next
            return count

    def len_until_cursor(self, cid: int) -> int:
        """Return the length until a cursor."""
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            cursor = self._cursors[cid]
            if cursor is None:
                raise IndexError

        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            count = 0
            node = self._head
            while node and node.prev is not cursor:
                count += 1
                node = node.next
            return count

    def len_until_first_cursor(self) -> int:
        """Return the length until the first cursor."""
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            count = 0
            node = self._head
            while node and not any(node.prev is cursor for cursor in self._cursors.values()):
                count += 1
                node = node.next
            return count

    def __getitem__(self, index: int | slice) -> T | list[T]:
        """Get an item."""
        node: SharedQueueNode[T] | None = None
        if isinstance(index, int):
            if index < 0:
                with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
                    node = self._tail
                    while node and index < -1:
                        index += 1
                        node = node.prev
                    if index < -1:
                        raise IndexError
            else:
                with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
                    node = self._head
                    while node and index > 0:
                        index -= 1
                        node = node.next
                    if index > 0:
                        raise IndexError

            if node is None:
                raise IndexError

            return node.value

        if isinstance(index, slice):
            raise NotImplementedError

        raise TypeError

    def to_list(self) -> list[T]:
        """Return as list."""
        lst = []
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            node = self._head
            while node:
                lst.append(node.value)
                node = node.next
        return lst

    def to_list_until_cursor(self, cid: int) -> list[T]:
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            cursor = self._cursors[cid]
            if cursor is None:
                raise IndexError

        lst = []
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            node = self._head
            while node and node is not cursor:
                lst.append(node.value)
                node = node.next
        return lst

    def register_consumer(self) -> int:
        with self._global_lock.gen_wlock():  # LIVENESS: Doesn't request another lock.
            cid = self._next_cid
            self._next_cid += 1
            self._cursors[cid] = None
            return cid

    def unregister_consumer(self, cid: int) -> None:
        with self._global_lock.gen_wlock():  # LIVENESS: Doesn't request another lock.
            self._cursors.pop(cid, None)

    def create_view(self) -> "SharedQueueView[T]":
        return SharedQueueView(queue=self, cid=self.register_consumer())

    def append(self, value: T) -> None:
        new_node = SharedQueueNode(value=value)
        with self._global_lock.gen_wlock():  # LIVENESS: Doesn't request another lock.
            if self._tail:
                self._tail.next = new_node
                new_node.prev = self._tail
                self._tail = new_node
            else:
                self._head = self._tail = new_node
        with self._new_data:
            self._new_data.notify_all()

    def drop_front(self, cnt: int = 1) -> None:
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            if cnt <= 0 or self._head is None:
                return

        with self._global_lock.gen_wlock():  # LIVENESS: Doesn't request another lock.
            node = self._head
            while node and cnt > 0:
                for cid, pos in self._cursors.items():
                    if node is pos:
                        self._cursors[cid] = None  # invalidate position
                cnt -= 1
                node = node.next
            if node is None:  # iterated beyond the tail
                self._head = None
                self._tail = None
            else:
                self._head = node
                node.prev = None

    def peek(self, cid: int) -> bool:
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock
            cursor = self._cursors[cid]
            if cursor is None:
                return self._head is not None
            return cursor.next is not None

    def next(self, cid: int) -> None:
        with self._new_data:
            with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
                cursor = self._cursors[cid]

            if cursor is None:

                def head_not_none() -> bool:
                    with (
                        self._global_lock.gen_rlock()
                    ):  # LIVENESS: Doesn't request another lock. Is only called before requesting the next wlock.
                        return self._head is not None

                self._new_data.wait_for(head_not_none)

                with (
                    self._global_lock.gen_wlock()
                ):  # LIVENESS: Doesn't request another lock (see liveness of head_not_none).
                    self._cursors[cid] = self._head
            else:

                def next_not_none() -> bool:
                    with (
                        self._global_lock.gen_rlock()
                    ):  # LIVENESS: Doesn't request another lock. Is only called before requesting the next wlock.
                        return cursor.next is not None

                self._new_data.wait_for(next_not_none)

                with (
                    self._global_lock.gen_wlock()
                ):  # LIVENESS: Doesn't request another lock (see liveness of next_not_none).
                    self._cursors[cid] = cursor.next

    def get_relative(self, cid: int, index: int) -> T:
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            cursor = self._cursors[cid]
            if cursor is None:
                raise IndexError

            node: SharedQueueNode[T] | None = None
            if index < 0:
                node = cursor
                while node and index < -1:
                    index += 1
                    node = node.prev
                if index < -1:
                    raise IndexError
            else:
                node = self._head
                while node and node is not cursor and index > 0:
                    index -= 1
                    node = node.next
                if index > 0:
                    raise IndexError

            if node is None:
                raise IndexError

            return node.value


class SharedQueueView(Generic[T]):
    _queue: SharedQueue[T]
    _cid: int

    def __init__(self, queue: SharedQueue[T], cid: int) -> None:
        self._queue = queue
        self._cid = cid

    def __getitem__(self, index: int | slice) -> T | list[T]:
        # SAFETY: From safety of SharedQueue.__getitem__.
        if isinstance(index, int):
            return self._queue.get_relative(cid=self._cid, index=index)
        if isinstance(index, slice):
            raise NotImplementedError
        raise TypeError

    def peek(self) -> bool:
        return self._queue.peek(cid=self._cid)

    def next(self) -> None:
        # SAFETY: From safety of SharedQueue.next.
        self._queue.next(cid=self._cid)

    def __len__(self) -> int:
        # SAFETY: From safety of SharedQueue.len_until_cursor.
        return self._queue.len_until_cursor(self._cid)

    def to_list(self) -> list[T]:
        # SAFETY: From safety of SharedQueue.to_list_until_cursor.
        return self._queue.to_list_until_cursor(self._cid)
