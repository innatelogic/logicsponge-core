"""Contains data structures for internal use in logicsponge-core."""

from threading import Condition
from typing import Generic, NewType, TypeVar

from readerwriterlock import rwlock

T = TypeVar("T")
CursorID = NewType("CursorID", int)


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
    _cursors: dict[CursorID, SharedQueueNode[T] | None]

    def __init__(self) -> None:
        """Create a SharedQueue object."""
        self._head = None
        self._tail = None

        self._global_lock = rwlock.RWLockFair()
        self._new_data = Condition()

        self._next_cid = 0
        self._cursors = {}

    def register_cursor(self) -> CursorID:
        """Register a new cursor.

        A cursor can point to a specific node in the queue. It can be advanced via next.

        Returns:
            the ID of the newly created cursor

        """
        with self._global_lock.gen_wlock():  # LIVENESS: Doesn't request another lock.
            cid = CursorID(self._next_cid)
            self._next_cid += 1
            self._cursors[cid] = None
            return cid

    def unregister_cursor(self, cid: CursorID) -> None:
        """Unregister a cursor.

        Args:
            cid: the ID of the cursor

        """
        with self._global_lock.gen_wlock():  # LIVENESS: Doesn't request another lock.
            self._cursors.pop(cid, None)

    def create_view(self) -> "SharedQueueView[T]":
        """Create a new SharedQueueView into the SharedQueue.

        Returns:
            the new SharedQueueView

        """
        return SharedQueueView(queue=self, cid=self.register_cursor())

    def __len__(self) -> int:
        """Return the length of the queue.

        Returns:
            the number of elements in the queue

        """
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            count = 0
            node = self._head
            while node:
                count += 1
                node = node.next
            return count

    def len_until_cursor(self, cid: CursorID) -> int:
        """Return the length of the queue until a given cursor.

        Args:
            cid: the cursor ID until which the length is computed

        Returns:
            the number of elements in the queue until the cursor's node

        """
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            cursor = self._cursors[cid]
            if cursor is None:
                return 0

        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            count = 0
            node = self._head
            while node and node.prev is not cursor:
                count += 1
                node = node.next
            return count

    def len_until_first_cursor(self) -> int:
        """Return the length until the least advanced cursor.

        Returns:
            the number of elements in the queue until the first cursor

        """
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            count = 0
            node = self._head
            while node and not any(node.prev is cursor for cursor in self._cursors.values()):
                count += 1
                node = node.next
            return count

    def __getitem__(self, index: int | slice) -> T | list[T]:
        """Get items from the queue.

        Args:
            index: the index of the item to be returned, or the slice of the items to be returned

        Returns:
            the item at the index, or the list of items of the slice

        Raises:
            IndexError: if index is invalid in the queue
            TypeError: if index is neither int nor slice

        """
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

    def get_relative(self, cid: CursorID, index: int) -> T:
        """Get items from the queue relative to a cursor.

        Args:
            cid: the ID of the cursor
            index: the relative index of the item to be returned

        Returns:
            the item at the relative index

        Raises:
            IndexError: if index is invalid in the queue

        """
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

    def to_list(self) -> list[T]:
        """Return the elements of the queue as a list.

        Returns:
             the list of elements

        """
        lst = []
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            node = self._head
            while node:
                lst.append(node.value)
                node = node.next
        return lst

    def to_list_until_cursor(self, cid: CursorID) -> list[T]:
        """Return the elements of the queue until a given cursor as a list.

        Args:
            cid: the ID of the cursor

        Returns:
             the list of elements

        """
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            cursor = self._cursors[cid]
            if cursor is None:
                return []

        lst = []
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            node = self._head
            while node and node is not cursor:
                lst.append(node.value)
                node = node.next
        return lst

    def append(self, value: T) -> None:
        """Append to the end of the queue.

        Args:
            value: the value to be appended

        """
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
        """Drop elements from the front.

        Args:
            cnt: the number of elements to drop

        """
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

    def peek(self, cid: CursorID) -> bool:
        """Check whether a cursor can be advanced without blocking.

        Args:
            cid: the ID of the cursor

        Returns:
            True iff a call to next() will not block due to waiting for a new data item

        """
        with self._global_lock.gen_rlock():  # LIVENESS: Doesn't request another lock
            cursor = self._cursors[cid]
            if cursor is None:
                return self._head is not None
            return cursor.next is not None

    def next(self, cid: CursorID) -> None:
        """Advance a cursor by one node.

        Blocks if the cursor is at the end of the queue.

        Args:
            cid: the ID of the cursor

        """
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


class SharedQueueView(Generic[T]):
    """View of a SharedQueue, i.e., a sub-queue with reduced capabilities."""

    _queue: SharedQueue[T]
    _cid: CursorID

    def __init__(self, queue: SharedQueue[T], cid: CursorID) -> None:
        """Initialize a SharedQueueView object."""
        self._queue = queue
        self._cid = cid

    def __len__(self) -> int:
        """Return the length of the view.

        Returns:
            the number of elements until the end of the view

        """
        # SAFETY: From safety of SharedQueue.len_until_cursor.
        return self._queue.len_until_cursor(self._cid)

    def to_list(self) -> list[T]:
        """Return the elements of the view as a list.

        Returns:
             the list of elements until the end of the view

        """
        # SAFETY: From safety of SharedQueue.to_list_until_cursor.
        return self._queue.to_list_until_cursor(self._cid)

    def __getitem__(self, index: int | slice) -> T | list[T]:
        """Get items from the SharedQueueView.

        Args:
            index: the index of the item to be returned, or the slice of the items to be returned

        Returns:
            the item at the index, or the list of items of the slice

        Raises:
            TypeError: if index is neither int nor slice

        """
        # SAFETY: From safety of SharedQueue.__getitem__.
        if isinstance(index, int):
            return self._queue.get_relative(cid=self._cid, index=index)
        if isinstance(index, slice):
            raise NotImplementedError
        raise TypeError

    def peek(self) -> bool:
        """Check whether the end of the view can be advanced without blocking due to blocking.

        Returns:
            True iff a call to next() will not block due to waiting for a new data item

        """
        return self._queue.peek(cid=self._cid)

    def next(self) -> None:
        """Advance the end of the view by one node.

        Blocks if the view is at the end of the underlying queue.

        """
        # SAFETY: From safety of SharedQueue.next.
        self._queue.next(cid=self._cid)
