"""Internal dataflow backend powered by Bytewax."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.run import cli_main

from logicsponge.core.logicsponge import DataItem, FlatMapTerm, FunctionTerm, SourceTerm, Term

if TYPE_CHECKING:
    from collections.abc import Iterable

    from logicsponge.core.graph import TermGraph


class IterablePartition(StatelessSourcePartition[DataItem]):
    """Partition over a static iterable of DataItems."""

    def __init__(self, items: Iterable[DataItem]) -> None:
        """Initialize partition with an iterable of DataItems."""
        self._iter = iter(items)

    def next_batch(self) -> Iterable[DataItem]:
        """Return next batch containing a single DataItem."""
        return [next(self._iter)]


def iterable_input(items: Iterable[DataItem]) -> DynamicSource:
    """Build a DynamicSource from an iterable of DataItems."""

    class _Inp(DynamicSource):
        """_Inp class."""

        def build(self, step_id: str, worker_index: int, worker_count: int) -> IterablePartition:
            """Build."""
            _ = (step_id, worker_index, worker_count)
            return IterablePartition(items)

    return _Inp()


class GeneratorSourcePartition(StatelessSourcePartition[DataItem]):
    """Partition that pulls directly from a SourceTerm's generator."""

    def __init__(self, term: SourceTerm) -> None:
        """Initialize partition with a SourceTerm."""
        self._term = term
        term.enter()
        self._generator = term.generate()

    def next_batch(self) -> Iterable[DataItem]:
        """Pull next DataItem from generator and return as batch."""
        try:
            item = next(self._generator)
            item.set_time_to_now()
        except StopIteration:
            self._term.exit()
            raise
        else:
            return [item]

    def close(self) -> None:
        """Clean up when partition is closed."""
        self._term.exit()


def source_input(term: SourceTerm) -> DynamicSource:
    """Build a DynamicSource that pulls directly from a SourceTerm's generator."""

    class _Inp(DynamicSource):
        """_Inp class."""

        def build(self, step_id: str, worker_index: int, worker_count: int) -> GeneratorSourcePartition:
            """Build."""
            _ = (step_id, worker_index, worker_count)
            return GeneratorSourcePartition(term)

    return _Inp()


def run_flow_graph(graph: TermGraph, *, flow_id: str = "logicsponge", workers: int | None = None) -> list[DataItem]:  # noqa: C901, PLR0915
    """Translate a TermGraph into a Bytewax dataflow and execute it."""
    flow = Dataflow(flow_id)
    term_nodes = graph._topological_order() if hasattr(graph, "_topological_order") else list(graph._terms)  # noqa: SLF001
    outputs: dict[Term, Any] = {}

    def sanitize(label: object) -> str:
        """Return a Bytewax-safe step id fragment."""
        text = str(label)
        return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in text)

    # Sources
    for term in term_nodes:
        if isinstance(term, SourceTerm):
            step_id = f"src-{sanitize(term.name)}-{id(term)}"
            # Use generator-based input (no threads!)
            outputs[term] = op.input(step_id, flow, source_input(term))

    # Functions
    already_keyed_terms = set()  # Track terms whose streams are already keyed
    for term in term_nodes:
        if term.processes_data():
            typed_term = cast("FunctionTerm | FlatMapTerm", term)
            upstream = [outputs[parent] for parent in graph._inbound.get(term, [])]  # noqa: SLF001
            if not upstream:
                continue

            if len(upstream) == 1:
                # Single input - pass through directly
                stream = upstream[0]
            else:
                # Multiple inputs - create hierarchical DataItems
                parent_terms = graph._inbound.get(term, [])  # noqa: SLF001
                parent_names = [parent.name for parent in parent_terms]
                expected_sources = set(parent_names)

                # Tag each stream with its source name
                tagged_streams = []
                for parent, parent_stream in zip(parent_terms, upstream, strict=False):
                    tagged = op.map(
                        f"tag-{sanitize(parent.name)}-{id(parent)}",
                        parent_stream,
                        lambda di, name=parent.name: (name, di),
                    )
                    tagged_streams.append(tagged)

                # Merge all tagged streams
                merged_tagged = op.merge(f"merge-tagged-{sanitize(term.name)}-{id(term)}", *tagged_streams)

                # Key everything with a fixed key for global state
                keyed = op.key_on(f"key-{sanitize(term.name)}-{id(term)}", merged_tagged, lambda _x: "sync")

                # Use stateful map to accumulate items and apply function
                def make_accumulator(expected: set[str], func_term: FunctionTerm | FlatMapTerm) -> Any:  # noqa: ANN401
                    """Make accumulator."""

                    def accumulate(
                        state: dict[str, DataItem] | None, item: tuple[str, DataItem]
                    ) -> tuple[dict[str, DataItem], list[DataItem]]:
                        """Accumulate."""
                        if state is None:
                            state = dict[str, DataItem]()
                        name, di = item
                        state[name] = di

                        # Check if we have items from all sources
                        if set(state.keys()) == expected:
                            # Emit hierarchical DataItem
                            hierarchical = DataItem(dict(state))
                            # Apply the function immediately
                            result = func_term.apply(hierarchical)
                            # Clear state for next batch
                            return ({}, result)
                        # Wait for more items
                        return (state, [])

                    return accumulate

                accumulated = op.stateful_map(
                    f"accumulate-{sanitize(typed_term.name)}-{id(typed_term)}",
                    keyed,
                    make_accumulator(expected_sources, typed_term),
                )

                # Flatten the batches (extract value from keyed stream and flatten list)
                outputs[typed_term] = op.flat_map(
                    f"flatten-{sanitize(typed_term.name)}-{id(typed_term)}", accumulated, lambda kv: kv[1]
                )
                # Mark this term as already processed (skip function application below)
                already_keyed_terms.add(typed_term)
                continue

            safe_name = sanitize(typed_term.name)
            step_prefix = f"{safe_name}-{id(typed_term)}"

            # Check if term is stateful - StatefulFunctionTerm or has special attributes
            # Stateful terms need sequential processing to avoid race conditions
            is_stateful = typed_term.requires_stateful() or hasattr(typed_term, "index")  # AddIndex

            if is_stateful and typed_term not in already_keyed_terms:
                # Use keyed stateful processing to ensure term instance variables are safe
                # Key stream so all items for this term go to same worker for sequential access
                term_id = id(typed_term)
                keyed_stream = op.key_on(f"key-{step_prefix}", stream, lambda _x, tid=term_id: f"term_{tid}")

                def make_stateful_apply(captured_term: FunctionTerm | FlatMapTerm) -> Any:  # noqa: ANN401
                    """Make stateful apply."""

                    def stateful_apply(_state: None, di: DataItem) -> tuple[None, list[DataItem]]:
                        """Apply function - state is unused, just ensures sequential execution."""
                        result_list = captured_term.apply(di)
                        return (None, result_list)

                    return stateful_apply

                # Apply stateful map (ensures sequential processing per term instance)
                stateful_mapped = op.stateful_map(f"f-{step_prefix}", keyed_stream, make_stateful_apply(typed_term))

                # Flatten the results (extract value from keyed stream and flatten list)
                outputs[typed_term] = op.flat_map(f"flatten-{step_prefix}", stateful_mapped, lambda kv: kv[1])
            else:
                outputs[typed_term] = op.flat_map(
                    f"f-{step_prefix}", stream, lambda di, term=typed_term: term.apply(di)
                )

    results: list[DataItem] = []

    class CollectorPartition(StatelessSinkPartition):
        """CollectorPartition class."""

        def write_batch(self, items: Iterable[Any]) -> None:
            """Write batch."""
            results.extend(items)

        def close(self) -> None:
            """Close."""
            return

    class CollectorSink(DynamicSink):
        """CollectorSink class."""

        def build(self, step_id: str, worker_index: int, worker_count: int) -> CollectorPartition:
            """Build."""
            _ = (step_id, worker_index, worker_count)
            return CollectorPartition()

    for term, stream in outputs.items():
        # A term is terminal if it has no outgoing edges (no downstream consumers)
        if not graph._edges.get(term):  # noqa: SLF001
            op.output(f"out-{term.name}-{id(term)}", stream, CollectorSink())

    cli_main(flow, workers_per_process=workers or 1)
    return results
