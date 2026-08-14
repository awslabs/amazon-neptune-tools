"""Multi-label node detection via targeted openCypher pair-existence queries.

Motivation
----------
The property-graph correlator assumes each node has exactly one label. Neptune
permits nodes with multiple labels; a multi-labeled node contributes to the
schema's ``nodeLabelDetails`` under *every* label it carries, which can silently
inflate or otherwise distort per-label counts derived from the summary API's
characteristic sets.

Detecting multi-labels via a full node scan (``MATCH (n) WHERE size(labels(n)) > 1``)
is O(num_nodes) and prohibitively expensive on large graphs. Instead, we probe
every unordered pair of labels ``(A, B)`` with ``MATCH (n:A:B) RETURN n LIMIT 1``.

Any node with three or more labels ``A::B::C`` must appear in each pair
sub-query it participates in, so pair-level existence checks are sufficient to
detect multi-labels of any arity.

Because Neptune edges may carry only a single label, this concern is
node-specific — there is no edge equivalent.

Query strategy
--------------
Two-phase to keep the common case (no multi-labels) cheap:

1. **Existence probe.** One openCypher query per invocation with a ``UNION ALL``
   branch per label pair. Each branch has its own ``LIMIT 1`` so the planner
   short-circuits at the first hit. Result rows identify pairs that have at
   least one match.
2. **Count.** For each pair that returned a hit, issue a targeted count query.
   Skipped entirely if no pairs matched.

Complexity: ``C(k, 2) + h`` queries where ``k`` is the label count and ``h`` is
the number of pairs with hits. For ``k=7``, that's 1 existence query + up to
21 count queries in the pathological case. Real graphs typically have h in the
low single digits.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Any

from neptune_schema_stats.client.base import NeptuneClient
from neptune_schema_stats.client.opencypher import execute_cypher, execute_cypher_scalar

# Guardrail: refuse to build a single UNION query with more than this many
# branches. Beyond this we chunk into multiple queries to avoid unbounded
# query length. C(k, 2) grows quadratically; 200 branches ≈ 20 labels.
MAX_UNION_BRANCHES = 200


@dataclass(frozen=True, slots=True)
class MultiLabelPair:
    """A pair of labels that has at least one node carrying both."""

    labels: tuple[str, str]
    node_count: int

    def display(self) -> str:
        return " | ".join(self.labels)


@dataclass(frozen=True, slots=True)
class MultiLabelProbeResult:
    """Outcome of a multi-label detection run over the graph's labels."""

    pairs_checked: int
    hits: tuple[MultiLabelPair, ...]

    @property
    def any_multi_label(self) -> bool:
        return bool(self.hits)

    @property
    def total_pair_intersections(self) -> int:
        """Sum of per-pair node counts.

        Note: a node with three labels A::B::C is counted under (A,B), (A,C),
        and (B,C), so this total *overcounts* distinct multi-labeled nodes.
        It is a useful order-of-magnitude signal, not an authoritative count.
        """
        return sum(p.node_count for p in self.hits)


def probe_multi_label_pairs(
    client: NeptuneClient,
    labels: list[str] | tuple[str, ...],
) -> MultiLabelProbeResult:
    """Detect multi-labeled nodes by probing all unordered label pairs.

    Uses a single ``UNION ALL`` query for existence detection, then follows up
    with per-hit count queries only for pairs that returned a match.

    :param client: An authenticated :class:`NeptuneClient`.
    :param labels: The set of node labels present in the graph (typically
        ``PGSummary.node_labels`` or ``PGSchema.node_labels``).
    :returns: A :class:`MultiLabelProbeResult` describing which pairs matched
        and their node counts.
    """
    unique_labels = sorted(set(labels))
    if len(unique_labels) < 2:
        # Need at least two distinct labels to have a multi-labeled node.
        return MultiLabelProbeResult(pairs_checked=0, hits=())

    pairs = list(combinations(unique_labels, 2))
    matched_pairs = _existence_probe(client, pairs)
    hits = tuple(
        MultiLabelPair(labels=pair, node_count=_count_pair(client, pair)) for pair in matched_pairs
    )
    return MultiLabelProbeResult(pairs_checked=len(pairs), hits=hits)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _existence_probe(
    client: NeptuneClient,
    pairs: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Return the subset of ``pairs`` that have at least one multi-labeled node.

    Uses a single ``UNION ALL`` query when the pair count fits within
    ``MAX_UNION_BRANCHES``; otherwise chunks into multiple queries.
    """
    if not pairs:
        return []

    # Map from the identifier we return per branch back to the label pair.
    # openCypher UNION requires identical column names; we use a synthetic
    # 'idx' integer to identify each branch, which is safer than embedding
    # label names as strings inside the query.
    matched: list[tuple[str, str]] = []
    for chunk_start in range(0, len(pairs), MAX_UNION_BRANCHES):
        chunk = pairs[chunk_start : chunk_start + MAX_UNION_BRANCHES]
        query = _build_existence_query(chunk, index_offset=chunk_start)
        body = execute_cypher(client, query)
        matched_indices = _parse_existence_response(body)
        matched.extend(pairs[i] for i in matched_indices)
    return matched


def _build_existence_query(pairs: list[tuple[str, str]], *, index_offset: int) -> str:
    """Build a single openCypher query with one UNION branch per pair.

    Each branch is of the form::

        MATCH (n:`A`:`B`) WITH n LIMIT 1 RETURN <idx> AS idx

    where ``<idx>`` is a monotonically increasing integer identifying the branch.
    """
    branches = [
        (
            f"MATCH (n:{_quote_label(a)}:{_quote_label(b)}) "
            f"WITH n LIMIT 1 RETURN {index_offset + i} AS idx"
        )
        for i, (a, b) in enumerate(pairs)
    ]
    return "\nUNION ALL\n".join(branches)


def _parse_existence_response(body: dict[str, Any]) -> list[int]:
    """Extract the branch indices that produced a match."""
    results = body.get("results", [])
    return [int(row["idx"]) for row in results if "idx" in row]


def _count_pair(client: NeptuneClient, pair: tuple[str, str]) -> int:
    a, b = pair
    query = f"MATCH (n:{_quote_label(a)}:{_quote_label(b)}) RETURN count(n) AS c"
    return execute_cypher_scalar(client, query)


def _quote_label(label: str) -> str:
    """Wrap a label in backticks, escaping any embedded backticks.

    Neptune's openCypher accepts backtick-quoted identifiers to permit labels
    with special characters. We escape any embedded backticks by doubling them,
    matching openCypher spec.
    """
    escaped = label.replace("`", "``")
    return f"`{escaped}`"
