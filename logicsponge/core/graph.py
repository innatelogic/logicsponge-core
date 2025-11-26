"""Explicit graph builder for terms.

Supports DAGs (and falls back gracefully for cycles) so execution order is
decided outside of the expression tree operators.
"""

from __future__ import annotations

from collections import defaultdict, deque

from logicsponge.core.logicsponge import DataStream, Term


class TermGraph:
    """Explicit term graph with configurable start order."""

    def __init__(self) -> None:
        """Initialize instance."""
        self._terms: set[Term] = set()
        self._edges: defaultdict[Term, list[Term]] = defaultdict(list)
        self._inbound: defaultdict[Term, list[Term]] = defaultdict(list)

    def add_term(self, term: Term) -> Term:
        """Register a term in the graph."""
        self._terms.add(term)
        return term

    def connect(self, producer: Term, consumer: Term, input_name: str, *, output_name: str | None = None) -> DataStream:
        """Wire producer output into a consumer input."""
        self.add_term(producer)
        self.add_term(consumer)

        stream = self._ensure_output_stream(producer, output_name)
        consumer._add_input(input_name, stream)  # noqa: SLF001
        self.connect_stream(stream, consumer)
        return stream

    def connect_stream(self, stream: DataStream, consumer: Term) -> None:
        """Register an existing stream as an edge into consumer."""
        producer = stream._owner  # noqa: SLF001
        self.add_term(producer)
        self.add_term(consumer)

        self._edges[producer].append(consumer)
        self._inbound[consumer].append(producer)

    def _ensure_output_stream(self, producer: Term, output_name: str | None) -> DataStream:
        """Ensure output stream."""
        name = output_name or producer.name
        stream = producer._outputs.get(name)  # noqa: SLF001
        if stream is None:
            stream = DataStream(owner=producer, label=f"{producer.name}:{name}")
            producer._outputs[name] = stream  # noqa: SLF001
        return stream

    def start(self, *, persistent: bool = False) -> None:
        """Start terms in reverse topological order (sinks first)."""
        topo = self._topological_order()
        started: set[Term] = set()

        for term in reversed(topo):
            term.start(persistent=persistent)
            started.add(term)

        for term in self._terms - started:
            # Cycle leftovers: just start them as-is.
            term.start(persistent=persistent)

    def stop(self) -> None:
        """Signal all terms to stop."""
        for term in self._terms:
            term.stop()

    def join(self) -> None:
        """Wait for all terms to finish."""
        for term in self._terms:
            term.join()

    def _topological_order(self) -> list[Term]:
        """Topological order."""
        indegree = {term: len(self._inbound[term]) for term in self._terms}
        queue: deque[Term] = deque(term for term, deg in indegree.items() if deg == 0)
        order: list[Term] = []

        while queue:
            term = queue.popleft()
            order.append(term)
            for nxt in self._edges.get(term, []):
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    queue.append(nxt)

        # If there's a cycle we'll return the partial order we managed to walk.
        return order if order else list(self._terms)
