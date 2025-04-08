from dataclasses import dataclass
from threading import Condition
from typing import Generic, TypeVar

from readerwriterlock import rwlock

T = TypeVar("T")


class SharedQueueNode(Generic[T]):
    value: T
    next: "SharedQueueNode | None"
    prev: "SharedQueueNode | None"

    def __init__(self, value: T) -> None:
        self.value = value
        self.next = None
        self.prev = None


class SharedQueue(Generic[T]):
    head: SharedQueueNode[T] | None
    tail: SharedQueueNode[T] | None

    global_lock: rwlock.RWLockFair
    new_data: Condition

    next_cid: int
    cursors: dict[int, SharedQueueNode[T] | None]

    def __init__(self) -> None:
        self.head = None
        self.tail = None

        self.global_lock = rwlock.RWLockFair()
        self.new_data = Condition()

        self.next_cid = 0
        self.cursors = {}

    def __len__(self) -> int:
        with self.global_lock.gen_rlock():
            count = 0
            node = self.head
            while node:
                count += 1
                node = node.next
            return count

    def len_until_first_cursor(self) -> int:
        with self.global_lock.gen_rlock():
            count = 0
            node = self.head
            while node and not any(node.prev is cursor for cursor in self.cursors.values()):
                count += 1
                node = node.next
            return count

    def __getitem__(self, index: int | slice) -> T | list[T]:
        node: SharedQueueNode[T] | None = None
        if isinstance(index, int):
            if index < 0:
                with self.global_lock.gen_rlock():
                    node = self.tail
                    while node and index < -1:
                        index += 1
                        node = node.prev
                    if index < -1:
                        raise IndexError
            else:
                with self.global_lock.gen_rlock():
                    node = self.head
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
        lst = []
        with self.global_lock.gen_rlock():
            node = self.head
            while node:
                lst.append(node.value)
                node = node.next
        return lst

    def register_consumer(self) -> int:
        with self.global_lock.gen_wlock():
            cid = self.next_cid
            self.next_cid += 1
            self.cursors[cid] = None
            return cid

    def unregister_consumer(self, cid: int) -> None:
        with self.global_lock.gen_wlock():
            self.cursors.pop(cid, None)

    def create_view(self) -> "SharedQueueView[T]":
        return SharedQueueView(queue=self, cid=self.register_consumer())

    def append(self, value: T) -> None:
        new_node = SharedQueueNode(value=value)
        with self.global_lock.gen_wlock():
            if self.tail:
                self.tail.next = new_node
                new_node.prev = self.tail
                self.tail = new_node
            else:
                self.head = self.tail = new_node
        with self.new_data:
            self.new_data.notify_all()

    def drop_front(self, cnt: int = 1) -> None:
        with self.global_lock.gen_rlock():
            if cnt <= 0 or self.head is None:
                return

        with self.global_lock.gen_wlock():
            node = self.head
            while node and cnt > 0:
                for cid, pos in self.cursors.items():
                    if node is pos:
                        self.cursors[cid] = None  # invalidate position
                cnt -= 1
                node = node.next
            if node is None:  # iterated beyond the tail
                self.head = None
                self.tail = None
            else:
                self.head = node
                node.prev = None

    def next(self, cid: int) -> None:
        with self.new_data:
            with self.global_lock.gen_rlock():
                cursor = self.cursors[cid]

            if cursor is None:

                def head_not_none() -> bool:
                    with self.global_lock.gen_rlock():
                        return self.head is not None

                self.new_data.wait_for(head_not_none)
                with self.global_lock.gen_wlock():
                    self.cursors[cid] = self.head
            else:

                def next_not_none() -> bool:
                    with self.global_lock.gen_rlock():
                        return cursor.next is not None

                self.new_data.wait_for(next_not_none)
                with self.global_lock.gen_wlock():
                    self.cursors[cid] = cursor.next

    def get_relative(self, cid: int, index: int) -> T:
        with self.global_lock.gen_rlock():
            cursor = self.cursors[cid]

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
                node = self.head
                while node and node is not cursor and index > 0:
                    index -= 1
                    node = node.next
                if index > 0:
                    raise IndexError

            if node is None:
                raise IndexError

            return node.value

    def _notify_all(self):
        pass


@dataclass
class SharedQueueView(Generic[T]):
    queue: SharedQueue[T]
    cid: int

    def __getitem__(self, index: int | slice) -> T | list[T]:
        if isinstance(index, int):
            return self.queue.get_relative(cid=self.cid, index=index)
        if isinstance(index, slice):
            raise NotImplementedError
        raise TypeError

    def next(self) -> None:
        self.queue.next(cid=self.cid)
