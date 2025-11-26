"""Contains the basic types of logicsponge-core (data items, data streams, terms, etc.)."""

import inspect
import logging
import pprint
import time
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Iterator
from datetime import datetime
from functools import reduce
from typing import Any, Self, TypeVar

from frozendict import frozendict
from readerwriterlock import rwlock
from typing_extensions import override

# logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    encoding="utf-8",
    format="%(message)s",
    level=logging.INFO,
)


State = dict[str, Any]


class LatencyQueue:
    """A queue of latencies, used to generate statistics of terms.

    Attributes
    ----------
        queue (deque): The queue of latencies.
        tic_time (float, optional): time of last tic() execution.

    """

    queue: deque
    tic_time: float | None

    def __init__(self, max_size: int = 100) -> None:
        """Create a LatencyQueue object.

        Args:
        ----
            max_size (int): the maximal size of the queue. Adding to a queue of this size, leads to
                the removal of the oldest entry.

        """
        self.tic_time = None
        self.queue = deque(maxlen=max_size)

    def tic(self) -> None:
        """Start a latency measurement.

        To be used with a successive toc().
        """
        # self.tic_time = datetime.now(UTC).timestamp()
        # make this faster via:
        self.tic_time = time.time()

    def toc(self) -> None:
        """Stop a latency measurement.

        Call after a corresponding tic().
        """
        # toc_time = datetime.now(UTC).timestamp()
        # make this faster via:
        toc_time = time.time()
        if self.tic_time is None:
            msg = "need to tic first"
            raise ValueError(msg)
        self.queue.append(toc_time - self.tic_time)
        self.tic_time = None

    @property
    def avg(self) -> float:
        """Average latency in seconds.

        Returns
        -------
            float: The average of all latencies in the queue. In [s].

        """
        latencies = list(self.queue)
        if self.tic_time is not None:
            # current_latency = datetime.now(UTC).timestamp() - self.tic_time
            # faster via:
            current_latency = time.time() - self.tic_time
            latencies += [current_latency]
        if not latencies:
            return float("nan")
        return sum(latencies) / len(latencies)

    @property
    def max(self) -> float:
        """Maximum latency in seconds.

        Returns
        -------
            float: The maximum of all latencies in the queue. In [s].

        """
        latencies = list(self.queue)
        if self.tic_time is not None:
            # current_latency = datetime.now(UTC).timestamp() - self.tic_time
            # faster via:
            current_latency = time.time() - self.tic_time
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

    def __init__(self, data: dict[str, Any] | Self | None = None) -> None:
        """Initialize a new DataItem.

        Args:
        ----
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
        self.set_time_to_now()  # set time to now

    @property
    def time(self) -> float:
        """The DataItem's time metadata.

        Returns
        -------
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

        Returns
        -------
            The DataITem after setting the time.

        """
        with self._lock.gen_wlock():
            # self._time = datetime.now(UTC).timestamp()
            # make faster via:
            self._time = time.time()
        return self

    def copy(self) -> "DataItem":
        """Copy the current DataItem with the current time as metadata.

        Returns
        -------
            The copied DataItem.

        """
        with self._lock.gen_rlock():
            new_copy = DataItem(self)
            new_copy.set_time_to_now()
            return new_copy

    def __str__(self) -> str:
        """Construct a string description of the DataItem.

        Returns
        -------
            A string of the format "DataItem(dict)" where dict contains all key/value data pairs.

        """
        return f"DataItem({self._data})"

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        """Return the data value for a given key.

        Args:
        ----
            key: The key of the data value to be returned.

        Returns:
        -------
            The data value.

        Raises:
        ------
            IndexError: If no data value exists for the given key.

        """
        try:
            return self._data[key]
        except IndexError as e:
            raise IndexError from e

    def __delitem__(self, key: str) -> None:
        """Delete the data value for a given key.

        Args:
        ----
            key: The key of the data value to be deleted.

        """
        new_data = dict(self._data)
        del new_data[key]
        self._data = frozendict(new_data)

    def __contains__(self, key: str) -> bool:
        """Check whether a data value for the given key exists in the DataItem.

        Args:
        ----
            key: The key to be checked.

        Returns:
        -------
            True if the DataItem contains a data value for the given key.

        """
        return key in self._data

    def __iter__(self) -> Iterator:
        """Construct an Iterator for the key/value data pairs.

        Returns
        -------
            The iterator.

        """
        return iter(self._data)

    def __len__(self) -> int:
        """Compute the number of data values in the DataItem.

        Returns
        -------
            The number of key/value data pairs in the DataItem.

        """
        return len(self._data)

    def __eq__(self, other: object) -> bool:
        """Check whether two DataItems have the same key/value data pairs.

        Args:
        ----
            other: The object to compare to.

        Returns:
        -------
            True if other is a DataItem and they contain the same key/value data pairs.
            False if other is a DataItem and they do not contain the same key/value data pairs.
            NotImplemented otherwise.

        """
        if isinstance(other, DataItem):
            return self._data == other._data
        return NotImplemented

    def __hash__(self) -> int:
        """Compute the hash of the DataItem.

        Returns
        -------
            The hash of the DataItem. Only takes into account key/value data pairs, not the metadata.

        """
        return hash(frozenset(self._data.items()))

    def items(self) -> Iterator[tuple[str, Any]]:
        """Construct an iterator for the key/value data pairs of the DataItem.

        Returns
        -------
            The iterator.

        """
        return iter(self._data.items())

    def keys(self) -> Iterator[str]:
        """Construct an iterator for the keys of the DataItem.

        Returns
        -------
            The iterator.

        """
        return iter(self._data.keys())

    def values(self) -> Iterator[Any]:
        """Construct an iterator for the data values of the DataItem.

        Returns
        -------
            The iterator.

        """
        return iter(self._data.values())

    def get(self, key: str, default: Any = None) -> Any:  # noqa: ANN401
        """Get the data value for the given key, or the default value if it doesn't exist.

        Args:
        ----
            key: The key of the requested data value.
            default: The default value to be returned if the data value doesn't exist.

        Returns:
        -------
            The data value for the key if it exists, default otherwise.

        """
        return self._data.get(key, default)


class DataStream:
    """Lightweight edge descriptor connecting terms in the dataflow graph.

    With bytewax backend, DataStreams are just metadata - the actual data flow
    is managed entirely by bytewax operators.
    """

    id: str | None
    _owner: "Term"
    _consumers: list["Term"]  # Terms that consume this stream
    label: str | None

    def __init__(self, owner: "Term", label: str | None = None) -> None:
        """Initialize a DataStream edge.

        Args:
        ----
            owner: The Term that produces data for this stream
            label: Optional human-readable label for debugging

        """
        self.id = None
        self._owner = owner
        self._consumers = []
        self.label = label

    def _set_id(self, new_id: str) -> Self:
        """Set ID of the DataStream.

        Args:
        ----
            new_id: The ID to be set.

        Returns:
        -------
            The DataStream after modification.

        """
        self.id = new_id
        return self

    def append(self, di: DataItem) -> Self:  # noqa: ARG002
        """No-op append for backward compatibility with threading-based sources.

        Bytewax backend doesn't use this - it pulls directly from generators.
        This exists only for legacy source code that still calls start() with threading.

        Args:
        ----
            di: The DataItem (ignored)

        Returns:
        -------
            self

        """
        # No-op: bytewax doesn't use append, pulls from generators instead
        return self

    def __str__(self) -> str:
        """Return string representation."""
        return f"DataStream(id={self.id}, owner={self._owner.name}, label={self.label})"


class Term(ABC):
    """The basic Term class.

    Attributes
    ----------
        name (str): The name of the Term. This will also be the name
            of the output stream.
        id (str, optional): Unique id of the Term, or None.

    """

    name: str
    id: str | None
    _outputs: dict[str, DataStream]
    _parent: "Term | None"

    def __init__(self, name: str | None = None, **kwargs) -> None:  # noqa: ANN003, ARG002
        """Create a Term."""
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
        """Compose the Term sequentially with the other Term."""
        if isinstance(other, Term):
            return SequentialTerm(self, other)
        msg = "Only terms can be combined in sequence"
        raise TypeError(msg)

    def __or__(self, other: "Term") -> "ParallelTerm":
        """Compose the Term in parallel with the other Term."""
        if isinstance(other, Term):
            return ParallelTerm(self, other)
        msg = "Only terms can be combined in parallel"
        raise TypeError(msg)

    @abstractmethod
    def start(self, *, persistent: bool = False) -> None:
        """Start execution of the term."""

    @abstractmethod
    def stop(self) -> None:
        """Signal a Term to stop its execution."""

    @abstractmethod
    def join(self) -> None:
        """Wait for a Term to terminate."""

    def cancel(self) -> None:
        """Cancel the Term."""
        return

    def connect_output_to(self, target: "Term", *, output_name: str | None = None, input_name: str) -> None:
        """Connect this term's output to another term's input.

        Args:
            target: The term to connect to
            output_name: Which output stream to use (defaults to self.name)
            input_name: What to call this input on the target term

        """
        if output_name is None:
            output_name = self.name

        if output_name not in self._outputs:
            msg = f"Output '{output_name}' not found in term {self.name}"
            raise ValueError(msg)

        stream = self._outputs[output_name]
        target._add_input(input_name, stream)  # noqa: SLF001

    def __str__(self) -> str:
        """Return a str representation."""
        return f"Term({self.name})"


class SourceTerm(Term):
    """Term that acts as a source - generates DataItems via iterator.

    With bytewax backend, SourceTerms provide a generate() method that yields DataItems.
    Bytewax pulls from this generator - no manual threading needed.
    """

    _output: DataStream
    state: State

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a SourceTerm object."""
        super().__init__(*args, **kwargs)
        self._output = DataStream(owner=self)
        self._outputs[self.name] = self._output
        self.state = {}

    def _add_input(self, name: str, ds: DataStream) -> None:  # noqa: ARG002
        msg = f"Cannot add inputs to a SourceTerm: '{name}'"
        raise ValueError(msg)

    def _set_id(self, new_id: str) -> None:
        self.id = new_id

        # set DataStream ID
        self._output._set_id(f"{self.id}:output")  # noqa: SLF001

    @abstractmethod
    def generate(self) -> Iterator[DataItem]:
        """Generate DataItems - bytewax pulls from this iterator.

        Override this method to produce DataItems. The bytewax backend will
        consume the iterator. No need to call output() or append to streams.

        Yields
        ------
            DataItems to be processed downstream

        """

    def enter(self) -> None:
        """Overwrite this function to initialize the source term."""

    def exit(self) -> None:
        """Overwrite this function to clean up the source term."""

    def start(self, *, persistent: bool = False) -> None:  # noqa: ARG002
        """Start execution using bytewax backend (synchronous, blocking)."""
        if not self.id:
            self._set_id("root")

        from logicsponge.core.flow_backend import run_flow_graph
        from logicsponge.core.graph import TermGraph

        # Collect all connected terms
        all_terms = self._collect_connected_terms()

        # Build graph
        graph = TermGraph()
        for term in all_terms:
            if not isinstance(term, CompositeTerm):
                graph.add_term(term)

        # Add connections
        for term in all_terms:
            if isinstance(term, FunctionTerm):
                for stream in term._inputs.values():  # noqa: SLF001
                    graph.connect_stream(stream, term)

        # Run bytewax dataflow (blocks until completion)
        run_flow_graph(graph)

    def _collect_connected_terms(self) -> list["Term"]:
        """Recursively collect all terms connected to this term."""
        collected = []
        visited = set()

        def visit(term: Term) -> None:
            if id(term) in visited:
                return
            visited.add(id(term))
            collected.append(term)

            # Visit children of composite terms
            if isinstance(term, CompositeTerm):
                visit(term.term_left)
                visit(term.term_right)

            # Visit downstream terms (consumers of output streams)
            for stream in term._outputs.values():  # noqa: SLF001
                for consumer in stream._consumers:  # noqa: SLF001
                    visit(consumer)

        visit(self)
        return collected

    def stop(self) -> None:
        """No-op stop for bytewax backend."""

    def join(self) -> None:
        """No-op join for bytewax backend (execution is synchronous)."""


class ConstantSourceTerm(SourceTerm):
    """Source term with pre-programmed list of data items."""

    _items: list[DataItem]

    def __init__(self, items: list[DataItem], *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a ConstantSourceTerm object.

        Args:
        ----
            items (list[DataItem]): List of data items to output.
            *args: Additional args.
            **kwargs: Additional kwargs.

        """
        super().__init__(*args, **kwargs)
        self._items = items

    def generate(self) -> Iterator[DataItem]:
        """Generate all items in the list."""
        yield from self._items


class FunctionTerm(Term):
    """A Term that receives data, performs a function on it, and outputs the resulting data.

    Attributes
    ----------
        state (State): The Term's state. Any state should go in here.

    """

    _inputs: dict[str, DataStream]  # name -> input stream
    _output: DataStream
    state: State

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a FunctionTerm object."""
        super().__init__(*args, **kwargs)
        self._inputs = {}
        self._output = DataStream(owner=self)
        self._outputs[self.name] = self._output
        self.state = {}

    def _add_input(self, name: str, ds: DataStream) -> None:
        if name in self._inputs:
            msg = f"Term {self.__class__}: tried to add input stream '{name}' but it already exists in the Term"
            raise ValueError(msg)
        self._inputs[name] = ds
        # Register self as a consumer of this stream
        if self not in ds._consumers:  # noqa: SLF001
            ds._consumers.append(self)  # noqa: SLF001

    def _set_id(self, new_id: str) -> None:
        self.id = new_id

        # set DataStream IDs
        self._output._set_id(f"{self.id}:output")  # noqa: SLF001
        for name, stream in self._inputs.items():
            stream._set_id(f"{self.id}:input:{name}")  # noqa: SLF001

    def f(self, di: DataItem) -> DataItem | None:
        """Process a single DataItem (may be hierarchical with nested DataItems).

        Args:
        ----
            di: Input DataItem. If term has multiple inputs with wait-for-all semantics,
                this will be a hierarchical DataItem like {"source1": DataItem(...), "source2": DataItem(...)}

        Returns:
        -------
            Processed DataItem, or None to filter out the item

        """
        raise NotImplementedError

    def enter(self) -> None:
        """Overwrite this function to initialize the function term."""

    def exit(self) -> None:
        """Overwrite this function to clean up the function term."""

    def stop(self) -> None:
        """No-op stop for bytewax backend."""

    def join(self) -> None:
        """No-op join for bytewax backend (execution is synchronous)."""

    def start(self, *, persistent: bool = False) -> None:  # noqa: ARG002
        """Start execution using bytewax backend (synchronous, blocking)."""
        if not self.id:
            self._set_id("root")

        from logicsponge.core.flow_backend import run_flow_graph
        from logicsponge.core.graph import TermGraph

        # Collect all connected terms
        all_terms = self._collect_connected_terms()

        # Build graph
        graph = TermGraph()
        for term in all_terms:
            if not isinstance(term, CompositeTerm):
                graph.add_term(term)

        # Add connections
        for term in all_terms:
            if isinstance(term, FunctionTerm):
                for stream in term._inputs.values():  # noqa: SLF001
                    graph.connect_stream(stream, term)

        # Run bytewax dataflow (blocks until completion)
        run_flow_graph(graph)

    def _collect_connected_terms(self) -> list["Term"]:
        """Recursively collect all terms connected to this term."""
        collected = []
        visited = set()

        def visit(term: Term) -> None:
            if id(term) in visited:
                return
            visited.add(id(term))
            collected.append(term)

            # Visit children of composite terms
            if isinstance(term, CompositeTerm):
                visit(term.term_left)
                visit(term.term_right)

            # Visit upstream terms (producers of input streams)
            if isinstance(term, FunctionTerm):
                for stream in term._inputs.values():  # noqa: SLF001
                    visit(stream._owner)  # noqa: SLF001

            # Visit downstream terms (consumers of output streams)
            for stream in term._outputs.values():  # noqa: SLF001
                for consumer in stream._consumers:  # noqa: SLF001
                    visit(consumer)

        visit(self)
        return collected


class StatefulFunctionTerm(FunctionTerm):
    """Marker class for FunctionTerms that require sequential processing.

    Bytewax will ensure DataItems are processed in order for StatefulFunctionTerms.
    Use this for terms that maintain state across DataItems and need ordering guarantees.
    """


class CompositeTerm(Term):
    """A Term that is composed of other Terms."""

    term_left: Term
    term_right: Term

    def __init__(self, term_left: Term, term_right: Term) -> None:
        """Create a CompositeTerm object."""
        super().__init__()
        self.term_left = term_left
        self.term_right = term_right

        self.term_left._parent = self  # noqa: SLF001
        self.term_right._parent = self  # noqa: SLF001

    def start(self, *, persistent: bool = False) -> None:  # noqa: ARG002
        """Start the Term using bytewax backend (synchronous, blocking)."""
        if not self.id:
            self._set_id("root")

        from logicsponge.core.flow_backend import run_flow_graph
        from logicsponge.core.graph import TermGraph

        # Collect all connected terms from both sides
        all_terms = self._collect_connected_terms()

        # Build graph
        graph = TermGraph()
        for term in all_terms:
            if not isinstance(term, CompositeTerm):
                graph.add_term(term)

        # Add connections
        for term in all_terms:
            if isinstance(term, FunctionTerm):
                for stream in term._inputs.values():  # noqa: SLF001
                    graph.connect_stream(stream, term)

        # Run bytewax dataflow (blocks until completion)
        run_flow_graph(graph)

    def _collect_connected_terms(self) -> list["Term"]:
        """Recursively collect all terms connected to this composite term."""
        collected = []
        visited = set()

        def visit(term: Term) -> None:
            if id(term) in visited:
                return
            visited.add(id(term))
            collected.append(term)

            # Visit children of composite terms
            if isinstance(term, CompositeTerm):
                visit(term.term_left)
                visit(term.term_right)

            # Visit upstream terms (producers of input streams)
            if isinstance(term, FunctionTerm):
                for stream in term._inputs.values():  # noqa: SLF001
                    visit(stream._owner)  # noqa: SLF001

            # Visit downstream terms (consumers of output streams)
            for stream in term._outputs.values():  # noqa: SLF001
                for consumer in stream._consumers:  # noqa: SLF001
                    visit(consumer)

        visit(self)
        return collected

    def stop(self) -> None:
        """Stop the Term."""
        # signal components to stop
        self.term_left.stop()
        self.term_right.stop()

    def join(self) -> None:
        """Wait until the Term terminates."""
        self.term_left.join()
        self.term_right.join()


class ParallelTerm(CompositeTerm):
    """Parallel composition of Terms."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a ParallelTerm object."""
        super().__init__(*args, **kwargs)
        # maybe copy?
        self._outputs = merge_dicts(self.term_left._outputs, self.term_right._outputs)  # noqa: SLF001

    def _add_input(self, name: str, ds: DataStream) -> None:
        # broadcast inputs
        self.term_left._add_input(name, ds)  # noqa: SLF001
        self.term_right._add_input(name, ds)  # noqa: SLF001

    def _set_id(self, new_id: str) -> None:
        # recursively set IDs in the syntax tree
        self.term_left._set_id(new_id + ":parallel-left")  # noqa: SLF001
        self.term_right._set_id(new_id + ":parallel-right")  # noqa: SLF001
        self.id = new_id

    def __str__(self) -> str:
        """Return as string."""
        str_left = str(self.term_left)[1:-1] if isinstance(self.term_left, ParallelTerm) else str(self.term_left)
        str_right = str(self.term_right)[1:-1] if isinstance(self.term_right, ParallelTerm) else str(self.term_right)
        return "(" + str_left + " | " + str_right + ")"


class SequentialTerm(CompositeTerm):
    """Sequential composition of Terms."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a SequentialTerm object."""
        super().__init__(*args, **kwargs)
        self.term_right._add_inputs(self.term_left._outputs)  # noqa: SLF001
        # maybe copy?
        self._outputs = self.term_right._outputs  # noqa: SLF001

    def _add_input(self, name: str, ds: DataStream) -> None:
        self.term_left._add_input(name, ds)  # noqa: SLF001

    def _set_id(self, new_id: str) -> None:
        # recursively set IDs in the syntax tree
        self.term_left._set_id(new_id + ":sequential-left")  # noqa: SLF001
        self.term_right._set_id(new_id + ":sequential-right")  # noqa: SLF001
        self.id = new_id

    def __str__(self) -> str:
        """Return as string."""
        str_left = str(self.term_left)[1:-1] if isinstance(self.term_left, SequentialTerm) else str(self.term_left)
        str_right = str(self.term_right)[1:-1] if isinstance(self.term_right, SequentialTerm) else str(self.term_right)
        return "(" + str_left + "; " + str_right + ")"


class Stop(Term):
    """Stops a data stream. It produces an empty output stream."""

    def _add_input(self, name: str, ds: DataStream) -> None:
        pass

    def _set_id(self, new_id: str) -> None:
        self.id = new_id

    def start(self, *, persistent: bool = False) -> None:  # noqa: ARG002
        """Start the Term."""
        if not self.id:
            self._set_id("root")

    def stop(self) -> None:
        """Stop the Term."""
        return

    def join(self) -> None:
        """Wait until Term terminates."""
        return


class Flatten(FunctionTerm):
    """Flattens the data item. The level of flattening can be specified."""

    level: int | None

    def __init__(self, *args, level: int | None = 1, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a Flatten object."""
        super().__init__(*args, **kwargs)
        self.level = level

    def f(self, di: DataItem) -> DataItem:
        """Execute f on new data - flattens nested dicts recursively."""
        return DataItem(flatten_dict(di))


class MergeToSingleStream(FunctionTerm):
    """Merges multiple streams into a single stream.

    When term has multiple inputs, receives hierarchical DataItem like:
    {"source1": DataItem(...), "source2": DataItem(...)}
    """

    def __init__(self, *args, combine: bool = False, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a MergeToSingleStream object."""
        super().__init__(*args, **kwargs)
        self.combine = combine

    def f(self, di: DataItem) -> DataItem:
        """Execute the Term's f."""
        if self.combine:
            # Combine all nested DataItems into single flat dict
            ret = {}
            for v in di.values():
                if isinstance(v, DataItem):
                    ret.update(v)
                else:
                    # Single input, just use di as-is
                    ret.update(di)
                    break
            return DataItem(ret)
        # Don't combine - pass through hierarchical DataItem
        return di


class KeyValueFilter(FunctionTerm):
    """Applies a key-value filter."""

    key_value_filter: Callable[[str, Any], bool] | None

    def __init__(self, *args, key_value_filter: Callable[[str, Any], bool] | None = None, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a new KeyValueFilter.

        Args:
        ----
            key_value_filter (Callable[[str, Any], bool], optional): Filter to be applied to
            each key-value pair in data item.

            args: Additional args.
            kwargs: Additional kwargs.

        """
        if key_value_filter is None and len(args) > 0 and has_callable_signature(args[0], (str, Any), bool):
            key_value_filter = args[0]  # type: ignore reportAssignmentType  # we check the signature above
            name = str(args[0])
            args = (name,) + args[1:]

        super().__init__(*args, **kwargs)
        self.key_value_filter = key_value_filter

    def f(self, item: DataItem) -> DataItem:
        """Run the f."""
        if self.key_value_filter is not None:
            filtered_attributes = {k: v for k, v in item.items() if self.key_value_filter(k, v)}
        else:
            filtered_attributes = item
        return DataItem(filtered_attributes)


class DataItemFilter(FunctionTerm):
    """Filters data items based on keys or not_keys."""

    data_item_filter: Callable[[DataItem], bool] | None

    def __init__(self, *args, data_item_filter: Callable[[DataItem], bool] | None = None, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a new DataItemFilter.

        key_value_filter: Callable[[DataItem], bool]
        Filter to be applied to each key-value pair in data item.
        """
        if data_item_filter is None and len(args) > 0 and has_callable_signature(args[0], (Any,), bool):
            data_item_filter = args[0]  # type: ignore reportAssignmentType  # we check the signature above
            name = str(args[0])
            args = (name,) + args[1:]

        super().__init__(*args, **kwargs)
        self.data_item_filter = data_item_filter

    def f(self, item: DataItem) -> DataItem | None:
        """Execute on new data."""
        if self.data_item_filter is None or self.data_item_filter(item):
            return item
        return None


class KeyFilter(KeyValueFilter):
    """Filters data items based on keys or not_keys."""

    keys: None | str | list[str]
    not_keys: None | str | list[str]

    def __init__(
        self,
        *args,  # noqa: ANN002
        keys: None | str | list[str] = None,
        not_keys: None | str | list[str] = None,
        **kwargs,  # noqa: ANN003
    ) -> None:
        """Create a new KeyFilter.

        keys: None, str, or list of str, optional
            Keys to include in the calculation.
        not_keys: None, str, or list of str, optional
            Keys to exclude from the calculation. Requires keyword argument.
        """
        if keys is not None and not_keys is not None:
            msg = "keys and not_keys cannot be specified simultaneously"
            raise ValueError(msg)

        if len(args) > 0 and keys is None and not_keys is None:
            if not isinstance(args[0], (str, list)):
                msg = "Interpreting first argument as keys. Expected str or list[str]."
                raise TypeError(msg)
            keys = args[0]
            name = str(args[0])
            args = (name,) + args[1:]

        self.keys = keys
        self.not_keys = not_keys

        if self.keys is not None:
            keys_list = [self.keys] if isinstance(self.keys, str) else self.keys

            def key_value_filter(key: str, value: Any) -> bool:  # noqa: ANN401, ARG001
                return key in keys_list

        else:
            not_keys_list = [self.not_keys] if isinstance(self.not_keys, str) else self.not_keys or []

            def key_value_filter(key: str, value: Any) -> bool:  # noqa: ANN401, ARG001
                return key not in not_keys_list

        super().__init__(*args, key_value_filter=key_value_filter, **kwargs)


class Print(FunctionTerm):
    """Prints selected keys of item, but outputs original item."""

    keys: None | str | list[str]
    not_keys: None | str | list[str]
    print_fun: Callable

    def __init__(
        self,
        *args,  # noqa: ANN002
        keys: None | str | list[str] = None,
        print_fun: Callable = print,
        date_to_str: bool = False,
        **kwargs,  # noqa: ANN003
    ) -> None:
        """Create a Print object."""
        self.keys = None
        self.not_keys = None
        not_keys = kwargs.pop("not_keys", None)

        self.print_fun = print_fun
        self.date_to_str = date_to_str

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
        """Execute the Term's f."""
        if self.keys:
            filtered_item = {k: item[k] for k in self.keys if k in item}
        elif self.not_keys:
            filtered_item = {k: v for k, v in item.items() if k not in self.not_keys}
        else:
            filtered_item = item

        formatted_item = {
            k: (v.strftime("%Y-%m-%d %H:%M:%S") if self.date_to_str and isinstance(v, datetime) else v)
            for k, v in filtered_item.items()
        }
        self.print_fun(formatted_item)
        return item  # return original item


class PPrint(Print):
    """Like Print, with using pprint.pprint."""

    def __init__(
        self,
        *args,  # noqa: ANN002
        keys: None | str | list[str] = None,
        print_fun: Callable = pprint.pprint,
        **kwargs,  # noqa: ANN003
    ) -> None:
        """Create a PPrint object."""
        super().__init__(*args, keys=keys, print_fun=lambda s: print_fun(s, **kwargs), **kwargs)


class PrintKeys(FunctionTerm):
    """Prints the keys in a data item. Outputs the original item."""

    print_fun: Callable

    def __init__(self, *args, print_fun: Callable = print, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a PrintKeys object."""
        self.print_fun = print_fun
        super().__init__(*args, **kwargs)

    def f(self, item: DataItem) -> DataItem:
        """Run the Term's f."""
        self.print_fun(list(item.keys()))
        return item  # return original item


class Dump(FunctionTerm):
    """Print the whole DataStream at every new item."""

    print_fun: Callable

    def __init__(self, *args, print_fun: Callable = print, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a Dump object."""
        self.print_fun = print_fun
        super().__init__(*args, **kwargs)

    def f(self, di: DataItem) -> DataItem:
        """Run the Term's f."""
        self.print_fun(di)
        return di


class Rename(FunctionTerm):
    """Renames keys."""

    fun: Callable[[str], str]

    def __init__(self, *args, fun: Callable[[str], str] | dict[str, str] = lambda x: x, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a Rename object."""
        super().__init__(*args, **kwargs)
        if isinstance(fun, dict):
            # rename via dict
            self.fun = lambda key: fun.get(key, key)
        else:
            # rename via function
            self.fun = fun

    def f(self, item: DataItem) -> DataItem:
        """Run the Term's f."""
        return DataItem({self.fun(k): v for k, v in item.items()})


class Id(FunctionTerm):
    """Identity. Id only forwards the 1st stream."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an Id object."""
        super().__init__(*args, **kwargs)

    def f(self, item: DataItem) -> DataItem:
        """Forward data."""
        return item


class AddIndex(FunctionTerm):
    """Add an index key."""

    key: str
    index: int

    def __init__(self, *args, key: str, index: int = 0, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an AddIndex object."""
        super().__init__(*args, **kwargs)
        self.key = key
        self.index = index

    def f(self, item: DataItem) -> DataItem:
        """Execute the Term's f."""
        di = DataItem({**item, self.key: self.index})
        self.index += 1
        return di


class Delay(FunctionTerm):
    """Delays a data stream."""

    delay_s: float

    def __init__(self, *args, delay_s: float, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an instance.

        Args:
        ----
            delay_s (float): The delay in seconds.
            args: Additional args.
            kwargs: Additional kwargs.

        """
        super().__init__(*args, **kwargs)
        self.delay_s = delay_s

    def f(self, item: DataItem) -> DataItem:
        """Run the Term's f."""
        time.sleep(self.delay_s)
        return item


KT = TypeVar("KT")
VT = TypeVar("VT")
# new syntax for generics according to PEP 695:
# def merge_dicts[KT, VT](outputs1: dict[KT, VT], outputs2: dict[KT, VT]) -> dict[KT, VT]:


def merge_dicts(outputs1: dict[KT, VT], outputs2: dict[KT, VT]) -> dict[KT, VT]:
    """Merge two dicts and return the merged one."""
    intersecting_keys = outputs1.keys() & outputs2.keys()
    for key in intersecting_keys:
        if outputs1[key] != outputs2[key]:
            msg = f"""Incompatible outputs detected for output stream '{key}'. \
            More than one term writes to this output stream."""
            raise ValueError(msg)
    return outputs1 | outputs2


def was_overwritten(f: Callable) -> bool:
    """Return if the argument was overwritten."""
    orig_code = FunctionTerm.f.__code__.co_code
    return f.__code__.co_code == orig_code


def get_annotations(obj: object, method_name: str) -> list | None:
    """Return the annotations of a method in an object."""
    if hasattr(obj, method_name):
        method = getattr(obj, method_name)
        if callable(method) and not was_overwritten(method):
            # Get the type hints of the method
            type_hints = inspect.signature(method).parameters
            # Ensure the type hints match the expected annotation
            return [param.annotation for param in type_hints.values()]

    return None


def parallel(li: list[Term]) -> Term:
    """Create a ParallelTerm for the arguments."""

    def red_parallel(x: Term, y: Term) -> ParallelTerm:
        return x | y

    return reduce(red_parallel, li)


def flatten_dict(d: dict[str, Any] | DataItem, parent_key: str | None = None, sep: str = ".") -> dict[str, Any]:
    """Return a flattened dict."""
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key is not None else k
        if isinstance(v, (DataItem, dict)):
            # recurse on sub-dict
            items.extend(flatten_dict(d=v, parent_key=new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def has_callable_signature(func: Any, args: tuple, ret: Any) -> bool:  # noqa: ANN401
    """Return if a func is callable."""
    if not callable(func):
        return False

    sig = inspect.signature(func)

    if len(sig.parameters) != len(args):
        return False

    for t_func, t_ref in zip((param.annotation for param in sig.parameters.values()), args, strict=True):
        if not (issubclass(t_ref, t_func) or t_func == Any):
            return False

    return issubclass(sig.return_annotation, ret) or ret == Any
