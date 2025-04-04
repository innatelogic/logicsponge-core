from collections.abc import Collection, Iterable, Iterator
from typing import Generic, Optional, TypeVar

T = TypeVar("T")


class DequeNode(Generic[T]):
    def __init__(self, value: T) -> None:
        self.value: T = value
        self.prev: DequeNode[T] | None = None
        self.next: DequeNode[T] | None = None

    def next_node(self) -> Optional["DequeNode[T]"]:
        return self.next

    def prev_node(self) -> Optional["DequeNode[T]"]:
        return self.prev

    def __repr__(self) -> str:
        return f"DequeNode({self.value!r})"


class Deque(Generic[T]):
    def __init__(self, iterable: Iterable[T] | None = None, maxlen: int | None = None) -> None:
        self.head: DequeNode[T] | None = None
        self.tail: DequeNode[T] | None = None
        self._length: int = 0
        self.maxlen: int | None = maxlen

        if iterable:
            for item in iterable:
                self.append(item)

    def _trim_to_maxlen(self) -> None:
        while self.maxlen is not None and self._length > self.maxlen:
            self.popleft()

    def append(self, value: T) -> DequeNode[T]:
        node = DequeNode(value)
        if not self.tail:
            self.head = self.tail = node
        else:
            node.prev = self.tail
            self.tail.next = node
            self.tail = node
        self._length += 1
        self._trim_to_maxlen()
        return node

    def appendleft(self, value: T) -> DequeNode[T]:
        node = DequeNode(value)
        if not self.head:
            self.head = self.tail = node
        else:
            node.next = self.head
            self.head.prev = node
            self.head = node
        self._length += 1
        self._trim_to_maxlen()
        return node

    def pop(self) -> T:
        if not self.tail:
            msg = "pop from an empty deque"
            raise IndexError(msg)
        node = self.tail
        self.tail = node.prev
        if self.tail:
            self.tail.next = None
        else:
            self.head = None
        self._length -= 1
        return node.value

    def popleft(self) -> T:
        if not self.head:
            msg = "pop from an empty deque"
            raise IndexError(msg)
        node = self.head
        self.head = node.next
        if self.head:
            self.head.prev = None
        else:
            self.tail = None
        self._length -= 1
        return node.value

    def clear(self) -> None:
        self.head = None
        self.tail = None
        self._length = 0

    def __len__(self) -> int:
        return self._length

    def __iter__(self) -> Iterator[T]:
        current = self.head
        while current:
            yield current.value
            current = current.next

    def __reversed__(self) -> Iterator[T]:
        current = self.tail
        while current:
            yield current.value
            current = current.prev

    def __getitem__(self, index: int | slice) -> T | list[T]:
        if isinstance(index, int):
            if index < 0:
                index += self._length
            if not (0 <= index < self._length):
                msg = "deque index out of range"
                raise IndexError(msg)

            node = self.head
            for _ in range(index):
                node = node.next  # type: ignore
            return node.value  # type: ignore

        if isinstance(index, slice):
            start, stop, step = index.indices(self._length)
            result: list[T] = []
            if step == 0:
                msg = "slice step cannot be zero"
                raise ValueError(msg)

            i = 0
            current = self.head
            while current and i < stop:
                if i >= start and (i - start) % step == 0:
                    result.append(current.value)
                current = current.next
                i += 1
            return result

        msg = "deque indices must be integers or slices"
        raise TypeError(msg)

    def __setitem__(self, index: int, value: T) -> None:
        if index < 0:
            index += self._length
        if not (0 <= index < self._length):
            msg = "deque index out of range"
            raise IndexError(msg)

        node = self.head
        for _ in range(index):
            node = node.next  # type: ignore
        node.value = value  # type: ignore

    def __repr__(self) -> str:
        return f"Deque([{', '.join(repr(v) for v in self)}])"

    def find_node(self, value: T) -> DequeNode[T] | None:
        current = self.head
        while current:
            if current.value == value:
                return current
            current = current.next
        return None

    def remove(self, value: T) -> DequeNode[T]:
        node = self.find_node(value)
        if not node:
            msg = f"{value!r} not in deque"
            raise ValueError(msg)

        if node.prev:
            node.prev.next = node.next
        else:
            self.head = node.next

        if node.next:
            node.next.prev = node.prev
        else:
            self.tail = node.prev

        self._length -= 1
        return node

    def slice_until_nodes(self, targets: Collection[DequeNode[T]], *, inclusive: bool = False) -> list[T]:
        """Return a list of values from head up to the first node in `targets`.

        If `inclusive` is True, the matching node is included.
        """
        result: list[T] = []
        targets = list(targets)
        current = self.head
        while current:
            if any(target is current for target in targets):
                if inclusive:
                    result.append(current.value)
                break
            result.append(current.value)
            current = current.next
        return result
