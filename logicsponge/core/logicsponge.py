"""Contains the basic types of logicsponge-core (data items, data streams, terms, etc.)."""

import inspect
import logging
import pprint
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Hashable, Iterator
from datetime import UTC, datetime
from enum import Enum
from functools import reduce
from typing import Any, Self, TypedDict, TypeVar

from frozendict import frozendict
from readerwriterlock import rwlock
from typing_extensions import override

from logicsponge.core.datastructures import SharedQueue, SharedQueueView

# logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    encoding="utf-8",
    format="%(message)s",
    level=logging.INFO,
)


# end of stream
class Control(Enum):
    """The possible control signals for DataItems.

    Attributes:
        EOS: The DataItem terminates the DataStream (End Of Stream).

    """

    EOS = "_EOS_"


State = dict[str, Any]


class LatencyQueue:
    queue: deque
    tic_time: float | None

    def __init__(self, max_size: int = 100) -> None:
        self.tic_time = None
        self.queue = deque(maxlen=max_size)

    def tic(self) -> None:
        self.tic_time = datetime.now(UTC).timestamp()

    def toc(self) -> None:
        toc_time = datetime.now(UTC).timestamp()
        if self.tic_time is None:
            msg = "need to tic first"
            raise ValueError(msg)
        self.queue.append(toc_time - self.tic_time)
        self.tic_time = None

    @property
    def avg(self) -> float:
        """Average latency in [s]"""
        latencies = list(self.queue)
        if self.tic_time is not None:
            current_latency = datetime.now(UTC).timestamp() - self.tic_time
            latencies += [current_latency]
        if not latencies:
            return float("nan")
        return sum(latencies) / len(latencies)

    @property
    def max(self) -> float:
        """Max latency in [s]"""
        latencies = list(self.queue)
        if self.tic_time is not None:
            current_latency = datetime.now(UTC).timestamp() - self.tic_time
            latencies += [current_latency]
        if not latencies:
            return float("nan")
        return max(latencies)


class DataItem:
    """Encapsulates a collection of key/value data pairs, together with associated metadata.

    Contains methods to keep track of timing and control data. The implementation is thread-safe.
    """

    _data: frozendict[str, Any]
    _lock: rwlock.RWLockFair

    _time: float
    _control: set[Control]

    def __init__(self, data: dict[str, Any] | Self | None = None) -> None:
        """Initialize a new DataItem.

        Args:
            data: Contains the data carried by the DataItem.
                If data is a dict, it copies the key/value data pairs out of the given dict.
                If data is a DataItem, it copies the key/value data pairs out of the given DataItem.
                If data is None, then the DataItem has no key/value data pairs.

        """
        # set data
        if data is None:
            self._data = frozendict()
        elif isinstance(data, DataItem):
            self._data = frozendict(data._data)  # noqa: SLF001
        else:
            self._data = frozendict(data)

        self._lock = rwlock.RWLockFair()

        self._control = set()  # by default not a control data item
        self.set_time_to_now()  # set time to now

    @property
    def has_control_signal(self) -> bool:
        """True iff the DataItem has a control signal."""
        with self._lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            return len(self._control) > 0

    def get_control_signal(self, control: Control) -> bool:
        """Check if a given control signal is set in the data item.

        Args:
            control: The control signal to be checked.

        Returns:
            True if the given control signal is set.

        """
        with self._lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            return control in self._control

    def set_control_signal(self, control: Control | set[Control]) -> Self:
        """Set the given control signal(s) in the DataItem.

        Args:
            control: The control signal(s).
                If control is a single Control, sets the signal in the DataItem.
                If control is a set of Control, sets all the set's signals in the DataItem.

        Returns:
            The DataItem after setting the control signal(s).

        """
        with self._lock.gen_wlock():
            self._control = control if isinstance(control, set) else {control}
        return self

    @property
    def time(self) -> float:
        """The DataItem's time metadata.

        Returns:
            The DataItem's time as a Unix timestamp.

        """
        with self._lock.gen_rlock():
            return self._time

    @time.setter
    def time(self, timestamp: float) -> None:
        with self._lock.gen_wlock():
            self._time = timestamp

    def set_time_to_now(self) -> Self:
        """Set the time metadata of the DataItem to the current time.

        Returns:
            The DataITem after setting the time.

        """
        with self._lock.gen_wlock():
            self._time = datetime.now(UTC).timestamp()
        return self

    def copy(self) -> "DataItem":
        """Copy the current DataItem with the current time as metadata.

        Returns:
            The copied DataItem.

        """
        with self._lock.gen_rlock():
            new_copy = DataItem(self)
            new_copy.set_time_to_now()
            new_copy.set_control_signal(self._control)
            return new_copy

    def __str__(self) -> str:
        """Construct a string description of the DataItem.

        Returns:
            A string of the format "DataItem(dict)" where dict contains all key/value data pairs.

        """
        return f"DataItem({self._data})"

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        """Return the data value for a given key.

        Args:
            key: The key of the data value to be returned.

        Returns:
            The data value.

        Raises:
            IndexError: If no data value exists for the given key.

        """
        try:
            return self._data[key]
        except IndexError as e:
            raise IndexError from e

    def __delitem__(self, key: str) -> None:
        """Delete the data value for a given key.

        Args:
            key: The key of the data value to be deleted.

        """
        new_data = dict(self._data)
        del new_data[key]
        self._data = frozendict(new_data)

    def __contains__(self, key: str) -> bool:
        """Check whether a data value for the given key exists in the DataItem.

        Args:
            key: The key to be checked.

        Returns:
            True if the DataItem contins a data value for the given key.

        """
        return key in self._data

    def __iter__(self) -> Iterator:
        """Construct an Iterator for the key/value data pairs.

        Returns:
            The iterator.

        """
        return iter(self._data)

    def __len__(self) -> int:
        """Compute the number of data values in the DataItem.

        Returns:
            The number of key/value data pairs in the DataItem.

        """
        return len(self._data)

    def __eq__(self, other: object) -> bool:
        """Check whether two DataItems have the same key/value data pairs.

        Args:
            other: The object to compare to.

        Returns:
            True if other is a DataItem and they contain the same key/value data pairs.
            False if other is a DataItem and they do not contain the same key/value data pairs.
            NotImplemented otherwise.

        """
        if isinstance(other, DataItem):
            return self._data == other._data
        return NotImplemented

    def __hash__(self) -> int:
        """Compute the hash of the DataItem.

        Returns:
            The hash of the DataItem. Only takes into account key/value data pairs, not the metadata.

        """
        return hash(frozenset(self._data.items()))

    def items(self) -> Iterator[tuple[str, Any]]:
        """Construct an iterator for the key/value data pairs of the DataItem.

        Returns:
            The iterator.

        """
        return iter(self._data.items())

    def keys(self) -> Iterator[str]:
        """Construct an iterator for the keys of the DataItem.

        Returns:
            The iterator.

        """
        return iter(self._data.keys())

    def values(self) -> Iterator[Any]:
        """Construct an iterator for the data values of the DataItem.

        Returns:
            The iterator.

        """
        return iter(self._data.values())

    def get(self, key: str, default: Any = None) -> Any:  # noqa: ANN401
        """Get the data value for the given key, or the default value if it doesn't exist.

        Args:
            key: The key of the requested data value.
            default: The default value to be returned if the data value doesn't exist.

        Returns:
            The data value for the key if it exists, default otherwise.

        """
        return self._data.get(key, default)


class ViewStatistics(TypedDict):
    """Represents the read/write statistics of a DataStreamView.

    Attributes:
        read: The number of read operations.
        write: The number of read operations.

    """

    read: int
    write: int


class HistoryBound(ABC):
    """Represents a bound on the DataStream history before which DataItems may be dropped."""

    @abstractmethod
    def items_to_drop(self, ds: "DataStream") -> int:
        """Calculate a bound on the DataStream history before which DataItems may be dropped.

        Is not allowed to call DataStream.clean_history.

        Args:
            ds: stream to be considered

        Returns:
            int: The length of the prefix of the history that can be deleted.

        """


class NoneBound(HistoryBound):
    """Marks no DataItems for deletion."""

    @override
    def items_to_drop(self, ds: "DataStream") -> int:
        """Calculate a bound on the DataStream history before which DataItems may be dropped.

        Args:
            ds: stream to be considered

        Returns:
            int: Always 0 (don't drop any items).

        """
        return 0


class NumberBound(HistoryBound):
    """Marks all but the n newest DataItems for deletion."""

    _n: int

    def __init__(self, n: int) -> None:
        """Construct a new NumberBound with a given value of n.

        Args:
            n: The length of the history to keep.

        """
        self._n = n

    @override
    def items_to_drop(self, ds: "DataStream") -> int:
        """Calculate a bound on the DataStream history before which DataItems may be dropped.

        Args:
            ds: stream to be considered

        Returns:
            int: The number of items to drop, i.e., max(0, len - n) where len is the length of the DataStream.

        """
        length = ds.len_until_first_cursor()
        if self._n <= length:
            return length - self._n
        return 0


class DataStream:
    """Represents a data stream, i.e., a sequence of DataItem, with associated metadata.

    The implementation is thread-safe.
    """

    _id: str | None
    _owner: "Term"

    _data: SharedQueue[DataItem]
    _lock: rwlock.RWLockFair

    _history_bound: HistoryBound
    _history_lock: threading.Lock

    def __init__(self, owner: "Term") -> None:
        """Initialize an empty DataStream.

        Args:
            owner: The Term that owns the DataStream. Usually the Term whose output stream we're initializing.
                Potentially used for persistence features.

        """
        self._id = None
        self._owner = owner

        self._data = SharedQueue()
        self._lock = rwlock.RWLockFair()

        self._history_bound = NoneBound()
        self._history_lock = threading.Lock()

    def _set_id(self, new_id: str) -> Self:
        """Set ID of the DataStream.

        Args:
            new_id: The ID to be set.

        Returns:
            The DataStream after modification.

        """
        with self._lock.gen_wlock():  # LIVENESS: Doesn't request another lock.
            self.id = new_id
        return self

    def get_id(self) -> str | None:
        """Get ID of the DataStream.

        Returns:
            The stream's ID or None if not set.

        """
        with self._lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            return self.id

    def __len__(self) -> int:
        """Calculate the length of the DataStream, i.e., the number of DataItems.

        Returns:
            The number of DataItems in the DataStream.

        """
        # SAFETY: From safety of SharedQueue.__len__.
        return len(self._data)

    def __getitem__(self, index: int | slice) -> DataItem | list[DataItem]:
        """Get DataItem(s) with a given index or slice.

        Args:
            index: The item(s) index or slice.

        Returns:
            The requested item(s). A single DataItem if index is an int. A list of DataItems if index is a slice.

        Raises:
            IndexError: If index is invalid.

        """
        # SAFETY: From safety of SharedQueue.__getitem__.
        try:
            return self._data[index]
        except IndexError as e:
            raise IndexError from e

    def __str__(self) -> str:
        """Construct a string description of the DataStream.

        Returns:
            A string of the format "DataStream(id={id}): [{data_items}]" where {id} is the DataStream's ID
                and {data_items} are the DataItems.

        """
        # SAFETY: Use of _lock for ID and from safety of SharedQueue.__len__.
        with self._lock.gen_rlock():
            return f"DataStream(id={self.get_id()}): {self._data.to_list()}"

    def append(self, di: DataItem) -> Self:
        """Append a data item to the end of the DataStream.

        Args:
            di: The DataItem to append.

        Returns:
            The DataStream after appending the DataItem.

        """
        # SAFETY: From safety of SharedQueue.append and clean_history.
        with (
            self._history_lock
        ):  # LIVENESS: Doesn't request another lock from DataStream, thus follows from liveness of SharedQueue.append.
            self._data.append(di)
        self.clean_history()
        return self

    def set_history_bound(self, history_bound: HistoryBound) -> Self:
        """Set the history bounds of the DataStream.

        Args:
            history_bound: The HistoryBound to be set.

        Returns:
            The DataStream after setting the history bound.

        """
        with self._lock.gen_wlock():  # LIVENESS: Doesn't request another lock.
            self._history_bound = history_bound
        self.clean_history()
        return self

    def clean_history(self) -> Self:
        """Drop DataItems according to the current history bound.

        If another call to clean_history or a call to append is currently running, returns without modification.

        Returns:
            The DataStream after dropping the DataItems.

        """
        # SAFETY: Use of mutex disallows concurrent modification of the SharedQueue.
        #         The value of cnt is thus consistent with the number of items to be dropped.
        have_lock = self._history_lock.acquire(blocking=False)  # LIVENESS: nonblocking
        if not have_lock:
            return self
        try:
            cnt = self._history_bound.items_to_drop(self)
            self._data.drop_front(cnt=cnt)
        finally:
            self._history_lock.release()
        return self

    def to_list(self) -> list[DataItem]:
        """Construct a list of all DataItems of the DataStream.

        Returns:
            The list of DataItems.

        """
        # SAFETY: From safety of SharedQueue.to_list.
        return self._data.to_list()

    def _create_queue_view(self) -> SharedQueueView[DataItem]:
        """Create a new associated SharedQueueView to the underlying SharedQueue.

        Returns:
            The new SharedQueueView.

        """
        # SAFETY: From safety of SharedQueue.create_view.
        return self._data.create_view()

    def len_until_first_cursor(self) -> int:
        """Calculate the length of the DataStream until the first cursor position of a associated SharedQueueView.

        Returns:
            The length until the first cursor.

        """
        # SAFETY: From safety of SharedQueue.len_until_first_cursor
        return self._data.len_until_first_cursor()


class DataStreamView:
    """Represents a view into a data stream, i.e., a prefix of the DataStream, with associated metadata.

    The implementation is thread-safe.
    """

    _id: str | None
    _owner: "Term"

    _ds: DataStream
    _view: SharedQueueView

    _lock: rwlock.RWLockFair

    def __init__(self, ds: DataStream, owner: "Term") -> None:
        """Initialize a new DataStreamView.

        Args:
            ds: The DataStream to which
            owner: The Term that owns the DataStream. Usually the Term whose output stream we're initializing.
                Potentially used for persistence features.

        """
        self._id = None
        self._owner = owner

        self._ds = ds
        self._view = ds._create_queue_view()  # noqa: SLF001

        self._lock = rwlock.RWLockFair()

    def _set_id(self, new_id: str) -> Self:
        """Set ID of the DataStreamView.

        Args:
            new_id: The ID to be set.

        Returns:
            The DataStreamView after modification.

        """
        with self._lock.gen_wlock():  # LIVENESS: Doesn't request another lock.
            self._id = new_id
        return self

    def get_id(self) -> str | None:
        """Get ID of the DataStreamView.

        Returns:
            The view's ID or None if not set.

        """
        with self._lock.gen_rlock():  # LIVENESS: Doesn't request another lock.
            return self._id

    def __len__(self) -> int:
        """Calculate the length of the DataStreamView, i.e., the number of DataItems until its cursor.

        Returns:
            The number of DataItems in the underlying DataStream until the DataStreamView's cursor.

        """
        # SAFETY: From safety of SharedQueueView.__len__.
        return len(self._view)

    def __getitem__(self, index: int | slice) -> DataItem | list[DataItem]:
        """Get DataItem(s) with a given index or slice.

        Args:
            index: The item(s) index or slice.

        Returns:
            The requested item(s). A single DataItem if index is an int. A list of DataItems if index is a slice.

        Raises:
            IndexError: If index is invalid.

        """
        # SAFETY: From safety of SharedQueueView.__getitem__.
        try:
            return self._view[index]
        except IndexError as e:
            raise IndexError from e

    def __str__(self) -> str:
        """Construct a string description of the DataStreamView.

        Returns:
            A string of the format "DataStreamView(id={id}): [{data_items}]" where {id} is the DataStreamView's ID
                and {data_items} are the DataItems.

        """
        # SAFETY: Use of _lock for ID and from safety of SharedQueueView.to_list.
        with self._lock.gen_rlock():
            return f"DataStreamView(id={self.get_id()}): {self._view.to_list()}"

    def next(self) -> None:
        """Advance the DataStreamView's cursor by one DataItem.

        Blocks if no item is available.

        """
        # SAFETY: From safety of SharedQueueView.next.
        self._view.next()

    def tail(self, n: int) -> list[DataItem]:
        raise NotImplementedError
        if n <= 0:
            raise IndexError(n)
        return self[-n:]

    @property
    def stats(self) -> ViewStatistics:
        raise NotImplementedError
        return {
            "read": self.pos,
            "write": len(self.ds),
        }

    def key_to_list(self, key: str, *, include_missing: bool = False) -> list[Any]:
        """Return the list of values associated with key in each DataItem
        (include_missing indicates whether None is included if the key/value doesn't exist).
        """
        raise NotImplementedError

        with self.ds.lock.gen_rlock():
            if include_missing:
                return [item.get(key, None) for item in self[:]]
            return [item[key] for item in self[:] if key in item]


class Term(ABC):
    name: str
    id: str | None
    _outputs: dict[str, DataStream]
    _parent: "Term | None"

    def __init__(self, name: str | None = None, **kwargs) -> None:  # noqa: ARG002
        # self.inputs: dict[str, DataStream] = {}
        # self.outputs: dict[str, DataStream] = {}
        self.id = None
        self._outputs = {}
        self._parent = None
        if name is None:
            self.name = str(type(self).__name__)
        else:
            self.name = name

    @abstractmethod
    def _add_input(self, name: str, ds: DataStream) -> None:
        pass

    def _add_inputs(self, dss: dict[str, DataStream]) -> None:
        for name, ds in dss.items():
            self._add_input(name, ds)

    @abstractmethod
    def _set_id(self, new_id: str) -> None:
        pass

    def __mul__(self, other: "Term") -> "SequentialTerm":
        if isinstance(other, Term):
            return SequentialTerm(self, other)
        msg = "Only terms can be combined in sequence"
        raise TypeError(msg)

    def __or__(self, other: "Term") -> "ParallelTerm":
        if isinstance(other, Term):
            return ParallelTerm(self, other)
        msg = "Only terms can be combined in parallel"
        raise TypeError(msg)

    @abstractmethod
    def start(self, *, persistent: bool = False):
        """Starts execution of the term"""

    @abstractmethod
    def stop(self):
        """Signals a Term to stop its execution."""

    @abstractmethod
    def join(self):
        """Waits for a Term to stop its execution."""

    def cancel(self):
        """Cancels the term"""

    def __str__(self) -> str:
        return f"Term({self.name})"


class SourceTerm(Term):
    _thread: threading.Thread | None
    _output: DataStream
    _stop_event: threading.Event
    state: State

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._output = DataStream(owner=self)
        self._outputs[self.name] = self._output
        self._thread = None  # initially no thread is running
        self._stop_event = threading.Event()
        self.state = {}

    def _add_input(self, name: str, ds: DataStream) -> None:  # noqa: ARG002
        msg = f"Cannot add inputs to a SourceTerm: '{name}'"
        raise ValueError(msg)

    def _set_id(self, new_id: str) -> None:
        self.id = new_id

        # set DataStream ID
        self._output._set_id(f"{self.id}:output")  # noqa: SLF001

    def run(self):
        """Overwrite this function to produce the source's output"""

    def enter(self):
        """Overwrite this function to initialize the source term's thread"""

    def exit(self):
        """Overwrite this function to clean up the source term's thread"""

    def start(self, *, persistent: bool = False):
        """Start the source term's thread"""
        if not self.id:
            self._set_id("root")

        def execute(stop_event: threading.Event):  # noqa: ARG001
            self.enter()
            try:
                self.run()
                self.eos()
            finally:
                self.exit()

        # check if no thread is already running
        if self._thread is None:
            # create a new thread
            self._thread = threading.Thread(target=execute, name=str(self), args=(self._stop_event,))
            self._thread.start()

    def stop(self):
        self._stop_event.set()
        logging.debug("%s stopped", self)

    def join(self):
        if self._thread is not None:
            self._thread.join()

    def output(self, data: DataItem) -> None:
        data.set_time_to_now()
        self._output.append(data)

    def eos(self) -> None:
        """Signal an EOS (end of stream) for this source."""
        eos_di = DataItem().set_control_signal(Control.EOS)
        self.output(eos_di)
        self.stop()

    @property
    def stats(self) -> dict:
        last_times = [di.time for di in self._output.to_list()]
        latency_avg = (last_times[-1] - last_times[0]) / len(last_times) if len(last_times) > 1 else float("nan")
        latency_max = (
            max(last_times[i] - last_times[i - 1] for i in range(1, len(last_times)))
            if len(last_times) > 1
            else float("nan")
        )
        term_type = self.__class__.__name__
        return {
            "name": self.name,
            "type": term_type,
            "output": {
                "id": self._output.id,
                "write": len(self._output),
                "latency_avg": latency_avg,
                "latency_max": latency_max,
            },
        }


class ConstantSourceTerm(SourceTerm):
    """Source term with pre-programmed list of data items."""

    _items: list[DataItem]

    def __init__(self, items: list[DataItem], *args, **kwargs):
        """items: list[DataItem]
        List of data items to output.
        """
        super().__init__(*args, **kwargs)
        self._items = items

    def run(self):
        for item in self._items:
            self.output(item)


class FunctionTerm(Term):
    _inputs: dict[str, DataStreamView]  # name -> input stream view
    _output: DataStream
    _thread: threading.Thread | None
    _stop_event: threading.Event
    _latency_queue: LatencyQueue
    state: State

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._inputs = {}
        self._output = DataStream(owner=self)
        self._outputs[self.name] = self._output
        self._thread = None  # initially no thread is running
        self._stop_event = threading.Event()
        self._latency_queue = LatencyQueue()
        self.state = {}

    def _add_input(self, name: str, ds: DataStream) -> None:
        if name in self._inputs:
            msg = f"Term {self.__class__}: tried to add input stream '{name}' but it already exists in the Term"
            raise ValueError(msg)
        self._inputs[name] = DataStreamView(ds=ds, owner=self)

    def _set_id(self, new_id: str) -> None:
        self.id = new_id

        # set DataStream IDs
        self._output._set_id(f"{self.id}:output")  # noqa: SLF001
        for name, ds in self._inputs.items():
            ds._set_id(f"{self.id}:input:{name}")  # noqa: SLF001

    def eos(self) -> None:
        """Signal an EOS (end of stream) for the function term."""
        eos_di = DataItem().set_control_signal(Control.EOS)
        self.output(eos_di)
        self.stop()

    def f(self, *args, **kwargs):
        raise NotImplementedError

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def stop(self):
        self._stop_event.set()
        logging.debug("%s stopped", self)

    def join(self):
        if self._thread is not None:
            self._thread.join()

    def enter(self):
        """Overwrite this function to initialize the function term's thread"""

    def exit(self):
        """Overwrite this function to clean up the function term's thread"""

    def start(self, *, persistent: bool = False):
        """Start the function term's thread"""
        if not self.id:
            self._set_id("root")

        # different execute versions
        def execute_f_dataitem(stop_event: threading.Event) -> None:
            # take the 1st stream if there are multiple input streams
            #   in case there are no input streams, set to None
            #   and stop execution
            input_stream: DataStreamView | None = next(iter(self._inputs.values()), None)
            if input_stream is None:
                return

            self.enter()
            try:
                while not stop_event.is_set():
                    input_stream.next()
                    self._latency_queue.tic()

                    last_di = input_stream[-1]
                    if last_di.get_control_signal(Control.EOS):
                        # received an EOS
                        self.eos()
                    else:
                        # pass the last data item from the stream to f
                        out = self.f(last_di)
                        if out is not None:
                            self.output(out)
                        # measure latency only for f(), not for control
                        self._latency_queue.toc()
            finally:
                self.exit()

        def execute_f_tuple_dataitem(stop_event: threading.Event) -> None:
            inputs: list[DataStreamView] = list(self._inputs.values())

            self.enter()
            try:
                while not stop_event.is_set():
                    for input_stream in inputs:
                        input_stream.next()
                    self._latency_queue.tic()

                    input_values = tuple(input_stream[-1] for input_stream in inputs)

                    if any(di.get_control_signal(Control.EOS) for di in input_values):
                        # we stop if any input stream signals EOS
                        self.eos()
                    else:
                        out = self.f(input_values)
                        if out is not None:
                            self.output(out)
                        # measure latency only for f(), not for control
                        self._latency_queue.toc()
            finally:
                self.exit()

        def execute_f_args_dataitem(stop_event: threading.Event) -> None:
            inputs: list[DataStreamView] = list(self._inputs.values())

            self.enter()
            try:
                while not stop_event.is_set():
                    for input_stream in inputs:
                        input_stream.next()
                    self._latency_queue.tic()
                    input_values = tuple([input_stream[-1] for input_stream in inputs])
                    if any(di.get_control_signal(Control.EOS) for di in input_values):
                        # we stop if any input stream signals EOS
                        self.eos()
                    else:
                        out = self.f(*input_values)
                        if out is not None:
                            self.output(out)
                        self._latency_queue.toc()
            finally:
                self.exit()

        def execute_f_dict_dataitem(stop_event: threading.Event) -> None:
            inputs: dict[str, DataStreamView] = self._inputs

            self.enter()
            try:
                while not stop_event.is_set():
                    for input_stream in inputs.values():
                        input_stream.next()
                    self._latency_queue.tic()
                    input_dict = {key: input_stream[-1] for key, input_stream in inputs.items()}
                    if any(di.get_control_signal(Control.EOS) for di in input_dict.values()):
                        # we stop if any input stream signals EOS
                        self.eos()
                    else:
                        out = self.f(input_dict)
                        if out is not None:
                            self.output(out)
                        self._latency_queue.toc()
            finally:
                self.exit()

        def execute_run_datastream(stop_event: threading.Event) -> None:  # noqa: ARG001
            inputs: DataStreamView | None = next(iter(self._inputs.values()), None)
            if inputs is None:
                return

            self.enter()
            try:
                self.run(inputs)
                self.eos()
            finally:
                self.exit()

        def execute_run_tuple_datastream(stop_event: threading.Event) -> None:  # noqa: ARG001
            inputs = tuple(self._inputs.values())

            self.enter()
            try:
                self.run(inputs)
                self.eos()
            finally:
                self.exit()

        def execute_run_args_datastream(stop_event: threading.Event) -> None:  # noqa: ARG001
            inputs = tuple(self._inputs.values())

            self.enter()
            try:
                self.run(*inputs)
                self.eos()
            finally:
                self.exit()

        def execute_run_dict_datastream(stop_event: threading.Event) -> None:  # noqa: ARG001
            inputs: dict[str, DataStreamView] = self._inputs

            self.enter()
            try:
                self.run(inputs)
                self.eos()
            finally:
                self.exit()

        # check which method was overwritten
        annotations_run = get_annotations(self, "run")
        annotations_f = get_annotations(self, "f")

        if annotations_run is None and annotations_f is None:
            msg = f"class {self.__class__}: overwrite run() or f()"
            raise ValueError(msg)

        if annotations_run is not None and annotations_f is not None:
            msg = f"class {self.__class__}: overwrite either run() or f(), but not both"
            raise ValueError(msg)

        execute = None
        if annotations_run is not None:
            if len(annotations_run) == 1:
                if annotations_run[0] == DataStreamView:
                    execute = execute_run_datastream
                elif annotations_run[0] == tuple[DataStreamView]:
                    execute = execute_run_tuple_datastream
                elif annotations_run[0] == dict[str, DataStreamView]:
                    execute = execute_run_dict_datastream
                else:
                    msg = f"class {self.__class__}: could not match run() with types {annotations_run}"
                    raise ValueError(msg)

            elif len(annotations_run) > 1:
                if all(param == DataStreamView for param in annotations_run):
                    execute = execute_run_args_datastream
                else:
                    msg = f"class {self.__class__}: could not match run() with types {annotations_run}"
                    raise ValueError(msg)
            else:
                ValueError(f"class {self.__class__}: could not match run() with types {annotations_run}")

        elif annotations_f is not None:
            if len(annotations_f) == 1:
                if annotations_f[0] == DataItem:
                    execute = execute_f_dataitem
                elif annotations_f[0] == tuple[DataItem]:
                    execute = execute_f_tuple_dataitem
                elif annotations_f[0] == dict[str, DataItem]:
                    execute = execute_f_dict_dataitem
                else:
                    msg = f"class {self.__class__}: could not match f() with types {annotations_f}"
                    raise ValueError(msg)

            elif len(annotations_f) > 1:
                if all(param == DataItem for param in annotations_f):
                    execute = execute_f_args_dataitem
                else:
                    msg = f"class {self.__class__}: could not match f() with types {annotations_run}"
                    raise ValueError(msg)

            else:
                msg = f"class {self.__class__}: could not match f() with types {annotations_f}"
                ValueError(msg)

        else:
            msg = f"class {self.__class__}: could not find f() or run() with supported types."
            raise ValueError(msg)

        if execute is None:
            msg = "should not happen"
            raise ValueError(msg)

        # check if no thread is already running
        if self._thread is None:
            # create a new thread
            self._thread = threading.Thread(target=execute, name=str(self), args=(self._stop_event,))
            self._thread.start()

    def next(self, input_stream: DataStreamView) -> None:
        input_stream.next()

    def output(self, data: DataItem | None) -> None:
        """Appends data to the output stream if data is not None"""
        if data is not None:
            self._output.append(data)

    @property
    def stats(self) -> dict:
        term_type = self.__class__.__name__
        return {
            "name": self.name,
            "type": term_type,
            "output": {
                "id": self._output.id,
                "write": len(self._output),
                "latency_avg": self._latency_queue.avg,
                "latency_max": self._latency_queue.max,
            },
            "inputs": {dsv.ds.id: {"read": dsv.pos, "write": len(dsv.ds)} for dsv in self._inputs.values()},
        }


class DynamicSpawnTerm(Term):
    """Spawns new terms for each unique filter_key. Dispatches the inputs and merges the outputs."""

    _filter_key: str
    _spawn_fun: Callable[[Hashable], Term]
    _spawned_streams: dict[Hashable, DataStream]
    _spawned_terms: dict[Hashable, Term]
    _thread: threading.Thread | None
    _output: DataStream
    _outputs: dict[str, DataStream]

    def __init__(self, filter_key: str, spawn_fun: Callable[[Hashable], Term], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._filter_key = filter_key
        self._spawn_fun = spawn_fun
        self._spawned_streams = {}
        self._spawned_terms = {}
        self._thread = None

        self._inputs = {}
        self._stop_event = threading.Event()
        self._latency_queue = LatencyQueue()
        self.state = {}
        self._output = DataStream(owner=self)
        self._outputs[self.name] = self._output

    def _add_input(self, name: str, ds: DataStream) -> None:
        if name in self._inputs:
            msg = f"Term {self.__class__}: tried to add input stream '{name}' but it already exists in the Term"
            raise ValueError(msg)
        self._inputs[name] = DataStreamView(ds=ds, owner=self)

    def _set_id(self, new_id: str) -> None:
        self.id = new_id

        # set DataStream IDs
        self._output._set_id(f"{self.id}:output")  # noqa: SLF001
        for name, ds in self._inputs.items():
            ds._set_id(f"{self.id}:input:{name}")  # noqa: SLF001

    def start(self, *, persistent: bool = False):
        if persistent:
            msg = "Persistence not implemented for DynamicSpawnTerms"
            raise NotImplementedError(msg)

        def execute(stop_event: threading.Event):  # noqa: ARG001
            inputs: DataStreamView | None = next(iter(self._inputs.values()), None)
            if inputs is None:
                return

            self.enter()
            try:
                self.run(inputs)
                self.eos()
            finally:
                self.exit()

        # check if no thread is already running
        if self._thread is None:
            # create a new thread
            self._thread = threading.Thread(target=execute, name=str(self), args=(self._stop_event,))
            self._thread.start()

    def run(self, ds: DataStreamView):
        ds.next()
        while True:  # until received EOS
            di = ds[-1]

            if di.get_control_signal(Control.EOS):
                for d in self._spawned_streams.values():
                    d.append(di)
                break

            iden = di[self._filter_key]
            if iden not in self._spawned_terms:
                new_ds = DataStream(self)
                eos_filter = EosFilter()
                new_term = self._spawn_fun(iden) * eos_filter
                new_term._set_id(f"id:{iden}")  # noqa: SLF001
                new_term._add_input(f"ds:{iden}", new_ds)  # noqa: SLF001
                eos_filter._outputs = self._outputs  # noqa: SLF001
                eos_filter._output = self._output  # type: ignore # noqa: SLF001 # TODO: new_term may not have _output
                new_term.start()

                self._spawned_streams[iden] = new_ds
                self._spawned_terms[iden] = new_term

            self._spawned_streams[iden].append(di)
            ds.next()

        # wait for all spawned terms to terminate
        for term in self._spawned_terms.values():
            term.join()

        self._output.append(DataItem().set_control_signal(Control.EOS))

    def eos(self) -> None:
        """Signal an EOS (end of stream) for the term."""
        eos_di = DataItem().set_control_signal(Control.EOS)
        self._output.append(eos_di)
        self.stop()

    def enter(self):
        """Overwrite this function to initialize the term's thread"""

    def exit(self):
        """Overwrite this function to clean up the term's thread"""

    def stop(self):
        self._stop_event.set()
        logging.debug("%s stopped", self)

    def join(self):
        if self._thread is not None:
            self._thread.join()

    @property
    def stats(self) -> dict:
        term_type = self.__class__.__name__
        return {
            "name": self.name,
            "type": term_type,
            "output": {
                "id": self._output.id,
                "write": len(self._output),
                "latency_avg": self._latency_queue.avg,
                "latency_max": self._latency_queue.max,
            },
            "inputs": {dsv.ds.id: {"read": dsv.pos, "write": len(dsv.ds)} for dsv in self._inputs.values()},
        }


class CompositeTerm(Term):
    term_left: Term
    term_right: Term

    def __init__(self, term_left: Term, term_right: Term) -> None:
        super().__init__()
        self.term_left = term_left
        self.term_right = term_right

        self.term_left._parent = self  # noqa: SLF001
        self.term_right._parent = self  # noqa: SLF001

    def start(self, *, persistent: bool = False):
        if not self.id:
            self._set_id("root")

        self.term_left.start(persistent=persistent)
        self.term_right.start(persistent=persistent)

    def stop(self) -> None:
        # signal components to stop
        self.term_left.stop()
        self.term_right.stop()

    def join(self):
        self.term_left.join()
        self.term_right.join()


class ParallelTerm(CompositeTerm):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # maybe copy?
        self._outputs = merge_dicts(self.term_left._outputs, self.term_right._outputs)  # noqa: SLF001

    def _add_input(self, name: str, ds: DataStream):
        # broadcast inputs
        self.term_left._add_input(name, ds)  # noqa: SLF001
        self.term_right._add_input(name, ds)  # noqa: SLF001

    def _set_id(self, new_id: str) -> None:
        # recursively set IDs in the syntax tree
        self.term_left._set_id(new_id + ":parallel-left")  # noqa: SLF001
        self.term_right._set_id(new_id + ":parallel-right")  # noqa: SLF001
        self.id = new_id

    def __str__(self) -> str:
        str_left = str(self.term_left)[1:-1] if isinstance(self.term_left, ParallelTerm) else str(self.term_left)
        str_right = str(self.term_right)[1:-1] if isinstance(self.term_right, ParallelTerm) else str(self.term_right)
        return "(" + str_left + " | " + str_right + ")"


class SequentialTerm(CompositeTerm):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.term_right._add_inputs(self.term_left._outputs)  # noqa: SLF001
        # maybe copy?
        self._outputs = self.term_right._outputs  # noqa: SLF001

    def _add_input(self, name: str, ds: DataStream):
        self.term_left._add_input(name, ds)  # noqa: SLF001

    def _set_id(self, new_id: str) -> None:
        # recursively set IDs in the syntax tree
        self.term_left._set_id(new_id + ":sequential-left")  # noqa: SLF001
        self.term_right._set_id(new_id + ":sequential-right")  # noqa: SLF001
        self.id = new_id

    def __str__(self) -> str:
        str_left = str(self.term_left)[1:-1] if isinstance(self.term_left, SequentialTerm) else str(self.term_left)
        str_right = str(self.term_right)[1:-1] if isinstance(self.term_right, SequentialTerm) else str(self.term_right)
        return "(" + str_left + "; " + str_right + ")"


class Stop(Term):
    """Stops a data stream. It produces an empty output stream."""

    def _add_input(self, name: str, ds: DataStream) -> None:
        pass

    def _set_id(self, new_id: str) -> None:
        self.id = new_id

    def start(self, *, persistent: bool = False):  # noqa: ARG002
        if not self.id:
            self._set_id("root")

    def stop(self) -> None:
        pass

    def join(self):
        return


class ToSingleStream(FunctionTerm):
    def __init__(self, *args, flatten: bool = False, merge: bool = False, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.flatten = flatten
        self.merge = merge

    def f(self, items: dict[str, DataItem]) -> DataItem:
        ret = next(iter(items.values())) if len(items) == 1 else items

        # merge all into a single dict
        if self.merge:
            after_merge = {}
            for v in ret.values():
                after_merge.update(v)
        else:
            after_merge = ret

        # return with data view names as keys
        return DataItem(flatten_dict(after_merge)) if self.flatten else DataItem(after_merge)


class Linearizer(FunctionTerm):
    linearized_input: DataStreamView  # contains linearized inputs together with global time stamp

    def __init__(self, *args, info: bool = True, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        ds = DataStream(owner=self)
        self.linearized_input = DataStreamView(ds=ds, owner=self)
        self.info = info

    def _callback_new_data(self, name, dataitem):
        """Callback function to be executed when new input data item arrives (see _add_input)."""
        dict_row = DataItem({"name": name, "data": dataitem}) if self.info else dataitem

        self.linearized_input.ds.append(dict_row)

    def _add_input(self, name: str, ds: DataStream):
        def new_data_callback(dataitem):
            self._callback_new_data(name, dataitem)

        ds.new_data_callbacks.append(new_data_callback)

    def run(self, dss: dict[str, DataStreamView]):
        _ = dss  # otherwise unused
        while True:
            self.linearized_input.next()
            self.output(self.linearized_input[-1])


class KeyValueFilter(FunctionTerm):
    """Applies a key-value filter."""

    key_value_filter: Callable[[str, Any], bool] | None

    def __init__(self, *args, key_value_filter: Callable[[str, Any], bool] | None = None, **kwargs) -> None:
        """key_value_filter: Callable[[str, Any], bool], optional
        Filter to be applied to each key-value pair in data item.
        """
        if key_value_filter is None and len(args) > 0 and has_callable_signature(args[0], (str, Any), bool):
            key_value_filter = args[0]
            args = args[1:]

        super().__init__(*args, **kwargs)
        self.key_value_filter = key_value_filter

    def f(self, item: DataItem) -> DataItem:
        if self.key_value_filter is not None:
            filtered_attributes = {k: v for k, v in item.items() if self.key_value_filter(k, v)}
        else:
            filtered_attributes = item
        return DataItem(**filtered_attributes)


class DataItemFilter(FunctionTerm):
    """Filters data items based on keys or not_keys."""

    data_item_filter: Callable[[DataItem], bool] | None

    def __init__(self, *args, data_item_filter: Callable[[DataItem], bool] | None = None, **kwargs) -> None:
        """key_value_filter: Callable[[DataItem], bool]
        Filter to be applied to each key-value pair in data item.
        """
        if data_item_filter is None and len(args) > 0 and has_callable_signature(args[0], (Any,), bool):
            data_item_filter = args[0]
            args = args[1:]

        super().__init__(*args, **kwargs)
        self.data_item_filter = data_item_filter

    def f(self, item: DataItem) -> DataItem | None:
        if self.data_item_filter is None or self.data_item_filter(item):
            return item
        return None


class KeyFilter(KeyValueFilter):
    """Filters data items based on keys or not_keys."""

    keys: None | str | list[str]
    not_keys: None | str | list[str]

    def __init__(
        self, *args, keys: None | str | list[str] = None, not_keys: None | str | list[str] = None, **kwargs
    ) -> None:
        """keys: None, str, or list of str, optional
            Keys to include in the calculation.
        not_keys: None, str, or list of str, optional
            Keys to exclude from the calculation. Requires keyword argument.
        """
        if keys is not None and not_keys is not None:
            msg = "keys and not_keys cannot be specified simultaneously"
            raise ValueError(msg)

        if len(args) > 0 and keys is None and not_keys is None:
            keys = args[0]
            args = args[1:]

        self.keys = keys
        self.not_keys = not_keys

        if self.keys is not None:
            keys_list = [self.keys] if isinstance(self.keys, str) else self.keys

            def key_value_filter(key: str, value: Any) -> bool:  # noqa: ARG001
                return key in keys_list

        else:
            not_keys_list = [self.not_keys] if isinstance(self.not_keys, str) else self.not_keys or []

            def key_value_filter(key: str, value: Any) -> bool:  # noqa: ARG001
                return key not in not_keys_list

        super().__init__(*args, key_value_filter=key_value_filter, **kwargs)


class Print(FunctionTerm):
    """Prints selected keys of item, but outputs original item."""

    keys: None | str | list[str]
    not_keys: None | str | list[str]
    print_fun: Callable

    def __init__(self, *args, keys: None | str | list[str] = None, print_fun: Callable = print, **kwargs):
        self.keys = None
        self.not_keys = None
        not_keys = kwargs.pop("not_keys", None)

        self.print_fun = print_fun

        if keys is None and len(args) > 0 and isinstance(args[0], str | list):
            keys = args[0]
            args = args[1:]

        if keys is not None and not_keys is not None:
            msg = "keys and not_keys cannot be specified simultaneously"
            raise ValueError(msg)

        super().__init__(*args, **kwargs)
        if keys is not None:
            self.keys = [keys] if isinstance(keys, str) else keys
        if not_keys is not None:
            self.not_keys = [not_keys] if isinstance(not_keys, str) else not_keys

    def f(self, item: DataItem) -> DataItem:
        if self.keys:
            filtered_item = {k: item[k] for k in self.keys if k in item}
        elif self.not_keys:
            filtered_item = {k: v for k, v in item.items() if k not in self.not_keys}
        else:
            filtered_item = item

        formatted_item = {
            k: (v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v) for k, v in filtered_item.items()
        }
        self.print_fun(formatted_item)
        return item  # return original item


class PPrint(Print):
    """Like Print, with using pprint.pprint."""

    def __init__(self, *args, keys: None | str | list[str] = None, print_fun: Callable = pprint.pprint, **kwargs):
        super().__init__(*args, keys=keys, print_fun=lambda s: print_fun(s, **kwargs), **kwargs)


class PrintKeys(FunctionTerm):
    """Prints the keys in a data item.
    Outputs the original item.
    """

    print_fun: Callable

    def __init__(self, *args, print_fun: Callable = print, **kwargs):
        self.print_fun = print_fun
        super().__init__(*args, **kwargs)

    def f(self, item: DataItem) -> DataItem:
        self.print_fun(list(item.keys()))
        return item  # return original item


class Dump(FunctionTerm):
    """Prints the whole DataStream at every new item"""

    print_fun: Callable

    def __init__(self, *args, print_fun: Callable = print, **kwargs):
        self.print_fun = print_fun
        super().__init__(*args, **kwargs)

    def f(self, di: DataItem):
        self.print_fun(di)


class Rename(FunctionTerm):
    """Renames keys."""

    fun: Callable[[str], str]

    def __init__(self, *args, fun: Callable[[str], str] | dict[str, str] = lambda x: x, **kwargs):
        super().__init__(*args, **kwargs)
        if isinstance(fun, dict):
            # rename via dict
            self.fun = lambda key: fun.get(key, key)
        else:
            # rename via function
            self.fun = fun

    def f(self, item: DataItem) -> DataItem:
        return DataItem({self.fun(k): v for k, v in item.items()})


class Id(FunctionTerm):
    """Identity"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def f(self, item: DataItem) -> DataItem:
        return item


class EosFilter(Id):
    """Removes all EOS data items from the data stream.

    Will stop at reception of EOS, but will not send one itself.
    This is almost always undesired behavior; use with caution.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def eos(self):
        self.stop()


class AddIndex(FunctionTerm):
    """Add index key."""

    key: str
    index: int

    def __init__(self, *args, key: str, index: int = 0, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.index = index

    def f(self, item: DataItem) -> DataItem:
        di = DataItem({**item, self.key: self.index})
        self.index += 1
        return di


class Delay(FunctionTerm):
    """Delays a data stream."""

    delay_s: float

    def __init__(self, *args, delay_s: float, **kwars) -> None:
        """Create an instance.

        Parameters
        ----------
        delay_s : float
            The delay in seconds.

        """
        super().__init__(*args, **kwars)
        self.delay_s = delay_s

    def f(self, item: DataItem) -> DataItem:
        time.sleep(self.delay_s)
        return item


KT = TypeVar("KT")
VT = TypeVar("VT")
# new syntax for generics according to PEP 695:
# def merge_dicts[KT, VT](outputs1: dict[KT, VT], outputs2: dict[KT, VT]) -> dict[KT, VT]:


def merge_dicts(outputs1: dict[KT, VT], outputs2: dict[KT, VT]) -> dict[KT, VT]:
    intersecting_keys = outputs1.keys() & outputs2.keys()
    for key in intersecting_keys:
        if outputs1[key] != outputs2[key]:
            msg = f"Incompatible outputs detected for output stream '{key}'. More than one term writes to this output stream."
            raise ValueError(msg)
    return outputs1 | outputs2


def was_overwritten(f: Callable) -> bool:
    orig_code = FunctionTerm.f.__code__.co_code
    return f.__code__.co_code == orig_code


def get_annotations(obj: object, method_name: str) -> list | None:
    if hasattr(obj, method_name):
        method = getattr(obj, method_name)
        if callable(method) and not was_overwritten(method):
            # Get the type hints of the method
            type_hints = inspect.signature(method).parameters
            # Ensure the type hints match the expected annotation
            return [param.annotation for param in type_hints.values()]

    return None


def parallel(li: list[Term]) -> Term:
    def red_parallel(x: Term, y: Term) -> ParallelTerm:
        return x | y

    return reduce(red_parallel, li)


def flatten_dict(d: dict[str, Any], parent_key: str | None = None, sep: str = ".") -> dict[str, Any]:
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key is not None else k
        if isinstance(v, dict):
            # recurse on sub-dict
            items.extend(flatten_dict(d=v, parent_key=new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def has_callable_signature(func: Any, args: tuple, ret) -> bool:
    if not callable(func):
        return False

    sig = inspect.signature(func)

    if len(sig.parameters) != len(args):
        return False

    for t_func, t_ref in zip((param.annotation for param in sig.parameters.values()), args, strict=True):
        if not (issubclass(t_ref, t_func) or t_func == Any):
            return False

    return issubclass(sig.return_annotation, ret) or ret == Any
