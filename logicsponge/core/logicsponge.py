import inspect
import itertools
import logging
import os
import pprint
import sys
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Hashable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from functools import reduce
from threading import Condition
from typing import Any, Self, TypedDict, TypeVar, overload

import persistent.list
import ZODB
import ZODB.FileStorage
from frozendict import frozendict
from readerwriterlock import rwlock

from logicsponge.core.datastructures import Deque, DequeNode

# logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    encoding="utf-8",
    format="%(message)s",
    level=logging.INFO,
)


# end of stream
class Control(Enum):
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
    _time: float
    _control: set[Control]
    _data: frozendict

    def __init__(self, data: dict | Self | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # by default not a control data item
        self._control = set()

        # set time to now
        self.to_now()

        # set data
        if data is None:
            self._data = frozendict()
        elif isinstance(data, DataItem):
            self._data = frozendict(data._data)  # noqa: SLF001
        else:
            self._data = frozendict(data)

    @property
    def is_control(self) -> bool:
        """Returns if the data item is a control item."""
        return len(self._control) > 0

    def is_set(self, control: Control) -> bool:
        """Returns if control is set in the data item."""
        return control in self._control

    def control(self, control: Control | set[Control]) -> Self:
        """Makes the data item a control item with the given control flags."""
        self._control = control if isinstance(control, set) else {control}
        return self

    @property
    def time(self) -> float:
        return self._time

    @time.setter
    def time(self, timestamp: float) -> None:
        self._time = timestamp

    def to_now(self) -> None:
        self._time = datetime.now(UTC).timestamp()

    def copy(self) -> "DataItem":
        """Creates a copy of the current DataItem with a new time."""
        new_copy = DataItem(self)
        new_copy.to_now()
        new_copy._control = self._control
        return new_copy

    def __repr__(self) -> str:
        return f"DataItem({dict(self._data)})"

    def __getitem__(self, key: Any) -> Any:
        return self._data[key]

    def __delitem__(self, key: Any) -> None:
        new_data = dict(self._data)
        del new_data[key]
        self._data = frozendict(new_data)

    def __contains__(self, key: Any) -> bool:
        return key in self._data

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DataItem):
            return self._data == other._data
        return False

    def __hash__(self) -> int:
        return hash(frozenset(self._data.items()))

    def items(self) -> Iterator[tuple[Any, Any]]:
        return iter(self._data.items())

    def keys(self) -> Iterator[Any]:
        return iter(self._data.keys())

    def values(self) -> Iterator[Any]:
        return iter(self._data.values())

    def get(self, key: Any, default: Any = None) -> Any:
        return self._data.get(key, default)


class ViewStatistics(TypedDict):
    read: int
    write: int


@dataclass
class DataStreamPersistentState(persistent.Persistent):
    data: persistent.list.PersistentList = field(default_factory=persistent.list.PersistentList)


class HistoryBound(ABC):
    """Calculates a bound on the DataStream history before which DataItems may be dropped."""

    @abstractmethod
    def bound_index(self, history: list[DataItem]) -> int:
        """
        Calculates a bound on the DataStream history before which DataItems may be dropped.

        Args:
            history (list[DataItem]): history to be considered

        Returns:
            int: The length of the prefix of the history that can be deleted.
        """


class NoneBound(HistoryBound):
    """Marks no DataItems for deletion."""

    def bound_index(self, history: list[DataItem]) -> int:
        return 0


class NumberBound(HistoryBound):
    """Marks all but the n newest DataItems for deletion."""

    n: int

    def __init__(self, n: int) -> None:
        self.n = n

    def bound_index(self, history: list[DataItem]) -> int:
        if self.n >= len(history):
            return len(history) - self.n
        return 0


class DataStream:
    id: None | str
    data: Deque[DataItem]
    lock: rwlock.RWLockFair
    new_data: Condition
    new_data_callbacks: list[Callable[[DataItem], None]]
    persistent: bool
    _owner: "Term"
    history_bound: HistoryBound
    _pos_dsvs: list[DequeNode]

    def __init__(self, owner: "Term") -> None:
        self.id = None
        self.data = Deque()
        self.lock = rwlock.RWLockFair()
        self.new_data = Condition()
        self.new_data_callbacks = []
        self.persistent = False
        self._owner = owner
        self.history_bound = NoneBound()
        self._pos_dsvs = []

    def _set_id(self, new_id: str) -> None:
        self.id = new_id

    def restore_state_from_db(self) -> None:
        raise NotImplementedError

    def __len__(self) -> int:
        """Return the number of rows in the DataStream."""
        with self.lock.gen_rlock():
            return len(self.data)

    @overload
    def __getitem__(self, index: int) -> DataItem:
        pass

    @overload
    def __getitem__(self, index: slice) -> list[DataItem]:
        pass

    def __getitem__(self, index: int | slice) -> DataItem | list[DataItem]:
        """Enable bracket notation for accessing rows."""
        if isinstance(index, int):
            return self.get_row(index)

        if isinstance(index, slice):
            return self.get_slice(index)

        # should not happen
        raise TypeError(index)

    def __str__(self) -> str:
        return str(self.to_list())

    def add_row(self, row_dict: DataItem):
        """Add a row to the end of the DataStream."""
        with self.lock.gen_wlock():
            if self.persistent:
                with self._owner.get_db().transaction() as conn:
                    root = conn.root()
                    root[self.id].data.append(row_dict)

            self.data.append(row_dict)

        # notify all that there is new data
        with self.new_data:
            self.new_data.notify_all()

        # apply all callback functions
        for func in self.new_data_callbacks:
            func(row_dict)

        self.clean_history()

    def get_row(self, index: int) -> DataItem:
        """Return a row by its index."""
        with self.lock.gen_rlock():
            return self.data[index]

    def get_slice(self, index: slice) -> list[DataItem]:
        """Returns the rows defined by the slice."""
        with self.lock.gen_rlock():
            start, stop, step = index.indices(len(self.data))
            return Deque(itertools.islice(self.data, start, stop, step))

    def set_history_bound(self, history_bound: HistoryBound) -> None:
        """
        Sets the history bounds of the DataStream.

        Args:
            history_bound (HistoryBound): Specifies the length of the prefix to be deleted. Gets the prefix until the minimal position of dependent DataStreamViews.
        """
        self.history_bound = history_bound
        self.clean_history()

    def clean_history(self) -> None:
        """Drops DataItems according to the current history bounds."""
        with self.lock.gen_rlock():
            prefix = self.data.slice_until_nodes(self._pos_dsvs, inclusive=True)
            index = self.history_bound.bound_index(prefix)

        with self.lock.gen_wlock():
            for _ in range(index):
                self.data.popleft()

    def from_dict_of_lists(self, dict_of_lists: dict[str, list]):
        """Load data from a dictionary of lists into the DataStream."""
        if not dict_of_lists:
            return
        num_rows = len(next(iter(dict_of_lists.values())))
        for i in range(num_rows):
            row_dict = DataItem({column: dict_of_lists[column][i] for column in dict_of_lists})
            self.add_row(row_dict)

    def to_list(self) -> list[DataItem]:
        """Return the entire DataStream as a list of dictionaries, including row numbers."""
        # Include row numbers starting from 0
        with self.lock.gen_rlock():
            return [DataItem({**row}) for _, row in enumerate(self.data)]


@dataclass
class DataStreamViewPersistentState(persistent.Persistent):
    pos: int = 0
    pos_requested: int = 0


class DataStreamView:
    id: str | None
    ds: DataStream
    pos: DequeNode[DataItem] | None
    persistent: bool
    _owner: "Term"

    def __init__(self, ds: DataStream, owner: "Term") -> None:
        self.id = None
        self.ds = ds
        self.pos = None
        self.persistent = False
        self._owner = owner

    def _set_id(self, new_id: str) -> None:
        self.id = new_id

    def restore_state_from_db(self) -> None:
        raise NotImplementedError

    def __len__(self) -> int:
        """Return the number of rows in the DataStreamView."""
        raise NotImplementedError

    @overload
    def __getitem__(self, index: int) -> DataItem:
        pass

    @overload
    def __getitem__(self, index: slice) -> list[DataItem]:
        pass

    def __getitem__(self, index: int | slice) -> DataItem | list[DataItem]:
        """Enable bracket notation for accessing rows."""
        if isinstance(index, int):
            if self.pos is None:
                raise IndexError(index)

            if index >= 0:
                with self.ds.lock.gen_rlock():
                    node = self.ds.data.head
                    for _ in range(index):
                        if node is None or node is self.pos:
                            raise IndexError(index)
                        node = node.next
                    if node is None:
                        raise IndexError(index)
                    return node.value
            else:
                with self.ds.lock.gen_rlock():
                    node = self.pos
                    if node is None:
                        raise IndexError(index)
                    for _ in range(-index - 1):
                        node = node.prev
                        if node is None:
                            raise IndexError(index)
                    return node.value

        if isinstance(index, slice):
            raise NotImplementedError

        # should not happen
        raise TypeError(index)

    def __str__(self) -> str:
        return f"DataStreamView: {[self.ds[i] for i in range(self.pos)]}"

    def next(self) -> None:
        with self.ds.new_data:
            if self.pos is None:
                self.ds.new_data.wait_for(lambda: self.ds.data.head is not None)

                self.pos = self.ds.data.head
            else:
                self.ds.new_data.wait_for(lambda: self.pos.next is not None)
                with self.ds.lock.gen_wlock():
                    self.ds._pos_dsvs = [pos for pos in self.ds._pos_dsvs if pos is not self.pos]  # noqa: SLF001
                self.pos = self.pos.next
            with self.ds.lock.gen_wlock():
                self.ds._pos_dsvs.append(self.pos)  # noqa: SLF001

    def tail(self, n: int) -> list[DataItem]:
        if n <= 0:
            raise IndexError(n)
        return self[-n:]

    @property
    def stats(self) -> ViewStatistics:
        return {
            "read": self.pos,
            "write": len(self.ds),
        }

    def key_to_list(self, key: str, *, include_missing: bool = False) -> list[Any]:
        """Return the list of values associated with key in each DataItem
        (include_missing indicates whether None is included if the key/value doesn't exist)."""

        with self.ds.lock.gen_rlock():
            if include_missing:
                return [item.get(key, None) for item in self[:]]
            return [item[key] for item in self[:] if key in item]


class Term(ABC):
    name: str
    id: str | None
    _outputs: dict[str, DataStream]
    _parent: "Term | None"
    _db: ZODB.DB | None

    def __init__(self, name: str | None = None, **kwargs) -> None:  # noqa: ARG002
        # self.inputs: dict[str, DataStream] = {}
        # self.outputs: dict[str, DataStream] = {}
        self.id = None
        self._outputs = {}
        self._parent = None
        self._db = None
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

    def _get_db_filename(self) -> str:
        basename, _ = os.path.splitext(os.path.basename(sys.argv[0]))
        return f"{basename}.fs"

    def get_db(self) -> ZODB.DB:
        # keep only one DB object: at the root
        if self._parent:
            return self._parent.get_db()

        if not self._db:
            storage = ZODB.FileStorage.FileStorage(self._get_db_filename())
            self._db = ZODB.DB(storage)

        return self._db

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
        """starts execution of the term"""

    @abstractmethod
    def stop(self):
        """Signals a Term to stop its execution."""

    @abstractmethod
    def join(self):
        """Waits for a Term to stop its execution."""

    def cancel(self):
        """cancels the term"""

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
        """overwrite this function to produce the source's output"""

    def enter(self):
        """overwrite this function to initialize the source term's thread"""

    def exit(self):
        """overwrite this function to clean up the source term's thread"""

    def start(self, *, persistent: bool = False):
        """start the source term's thread"""

        if not self.id:
            self._set_id("root")

        self._output.persistent = persistent
        # self._output.restore_state_from_db()

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
        data.to_now()
        self._output.add_row(data)

    def eos(self) -> None:
        """Signal an EOS (end of stream) for this source."""
        eos_di = DataItem().control(Control.EOS)
        self.output(eos_di)
        self.stop()

    @property
    def stats(self) -> dict:
        last_times = [di.time for di in self._output]
        latency_avg = (last_times[-1] - last_times[0]) / len(last_times) if len(last_times) > 1 else float("nan")
        latency_max = (
            max([last_times[i] - last_times[i - 1] for i in range(1, len(last_times))])
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
        """
        items: list[DataItem]
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
        eos_di = DataItem().control(Control.EOS)
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
        """overwrite this function to initialize the function term's thread"""

    def exit(self):
        """overwrite this function to clean up the function term's thread"""

    def start(self, *, persistent: bool = False):
        """start the function term's thread"""

        if not self.id:
            self._set_id("root")

        self._output.persistent = persistent
        # self._output.restore_state_from_db()

        for ds in self._inputs.values():
            ds.persistent = persistent
            # ds.restore_state_from_db()

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
                    if last_di.is_set(Control.EOS):
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

                    input_values = tuple([input_stream[-1] for input_stream in inputs])

                    if any(di.is_set(Control.EOS) for di in input_values):
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
                    if any(di.is_set(Control.EOS) for di in input_values):
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
                    if any(di.is_set(Control.EOS) for di in input_dict.values()):
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
        """appends data to the output stream if data is not None"""
        if data is not None:
            self._output.add_row(data)

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
    """
    Spawns new terms for each unique filter_key. Dispatches the inputs and merges the outputs.
    """

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

            if di.is_set(Control.EOS):
                for d in self._spawned_streams.values():
                    d.add_row(di)
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

            self._spawned_streams[iden].add_row(di)
            ds.next()

        # wait for all spawned terms to terminate
        for term in self._spawned_terms.values():
            term.join()

        self._output.add_row(DataItem().control(Control.EOS))

    def eos(self) -> None:
        """Signal an EOS (end of stream) for the term."""
        eos_di = DataItem().control(Control.EOS)
        self._output.add_row(eos_di)
        self.stop()

    def enter(self):
        """overwrite this function to initialize the term's thread"""

    def exit(self):
        """overwrite this function to clean up the term's thread"""

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

        self.linearized_input.ds.add_row(dict_row)

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
        """
        key_value_filter: Callable[[str, Any], bool], optional
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
        """
        key_value_filter: Callable[[DataItem], bool]
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
        """
        keys: None, str, or list of str, optional
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
    Outputs the original item."""

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
    """
    Removes all EOS data items from the data stream.

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
    """
    Delays a data stream.
    """

    delay_s: float

    def __init__(self, *args, delay_s: float, **kwars) -> None:
        """
        Create an instance.

        Parameters:
        -----------
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
