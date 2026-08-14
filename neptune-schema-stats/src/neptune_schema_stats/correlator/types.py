"""Dataclass definitions produced and consumed by the property-graph correlator.

Correlation logic lives in :mod:`neptune_schema_stats.correlator.pg`. This
module only defines the data structures — it has no runtime behavior and
imports only from :mod:`neptune_schema_stats.models` (the raw API response
types) and the standard library.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from neptune_schema_stats.models import PGEdgeStructure, PGNodeStructure

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LabelSignature:
    """Structural fingerprint of a single label derived from the PG schema."""

    label: str
    properties: frozenset[str]
    valid_outgoing: frozenset[str]
    valid_incoming: frozenset[str]


@dataclass(frozen=True, slots=True)
class NodeMatch:
    """A node characteristic set together with the labels it could belong to."""

    structure: PGNodeStructure
    candidate_labels: tuple[str, ...]

    @property
    def is_exact(self) -> bool:
        return len(self.candidate_labels) == 1

    @property
    def is_ambiguous(self) -> bool:
        return len(self.candidate_labels) > 1

    @property
    def is_unmapped(self) -> bool:
        return len(self.candidate_labels) == 0


@dataclass(frozen=True, slots=True)
class LabelStats:
    """Per-label derived statistics attributed from *exact* structure matches only."""

    label: str
    node_count: int
    contributing_structures: int
    property_fill_counts: dict[str, int]
    """Number of matched nodes that have each schema-declared property populated.

    Keys are always the full property set of the label from the schema. The
    value is the count of nodes (across contributing structures) whose
    characteristic set included that property. Compute a fill *rate* by
    dividing by ``node_count``.
    """


@dataclass(frozen=True, slots=True)
class AmbiguousNodeGroup:
    """A cluster of structures sharing the same candidate label set (>1 labels)."""

    candidate_labels: tuple[str, ...]
    structures: tuple[PGNodeStructure, ...]
    total_count: int


@dataclass(frozen=True, slots=True)
class EdgeMatch:
    """An edge characteristic set together with the edge labels it could belong to."""

    structure: PGEdgeStructure
    candidate_labels: tuple[str, ...]

    @property
    def is_exact(self) -> bool:
        return len(self.candidate_labels) == 1

    @property
    def is_ambiguous(self) -> bool:
        return len(self.candidate_labels) > 1

    @property
    def is_unmapped(self) -> bool:
        return len(self.candidate_labels) == 0


@dataclass(frozen=True, slots=True)
class EdgeStats:
    """Per-edge-label statistics from an exact match."""

    label: str
    edge_count: int
    property_signature: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ImplicitEmptyEdges:
    """The residual edge count with no explicit characteristic set.

    Neptune's summary API often omits the empty-property edge structure. The
    residual (``num_edges - sum(explicit)``) is attributed to edge labels
    whose schema declares zero properties.
    """

    count: int
    candidate_labels: tuple[str, ...]

    @property
    def is_exact(self) -> bool:
        return len(self.candidate_labels) == 1

    @property
    def is_ambiguous(self) -> bool:
        return len(self.candidate_labels) > 1


@dataclass(frozen=True, slots=True)
class EdgeLabelBound:
    """Per-edge-label bound on the number of edges of that type in the graph.

    ``min_count`` is a **strict lower bound**, derived by one of two paths
    depending on the label's property signature:

    - **Unique signature** (the common case): ``min_count`` equals the exact
      total of the summary's matching edge structures for that signature.
      Since no other label shares the signature, every one of those edges
      must carry this label.
    - **Shared signature** (typically the empty-signature edge case):
      ``min_count`` is derived from node characteristic sets. If a node
      structure declares an outgoing edge type ``E``, every node in it emits
      at least one outgoing ``E`` edge, contributing ``structure.count`` to
      the floor for ``E``.

    ``max_count`` is a **strict upper bound**:

    - For an edge label with a unique property signature in the schema,
      ``max_count == min_count == the exact edge count`` from the summary's
      matching edge structure.
    - For an edge label whose property signature is shared with other labels
      (typically the empty-signature case), ``max_count`` equals the total
      residual for that signature minus the sum of the other candidate
      labels' minimums.

    ``source_labels`` lists the node labels permitted to source this edge
    type per ``labelTriples`` from the schema. It is deduplicated across
    triples; use ``source_target_pairs`` when you need to preserve the
    actual ``(source, target)`` pairings for rendering.

    ``source_target_pairs`` is a deduplicated tuple of ``(from_label,
    to_label)`` pairs drawn directly from the schema's ``labelTriples`` for
    this edge type. Multiple entries mean the edge type is used in more
    than one distinct pattern (e.g. ``visited: transientId→website`` AND
    ``visited: persistentId→mobileApp``). Pairs are sorted for stable
    display.

    ``is_exact`` is ``True`` when ``min_count == max_count`` — typically
    because the edge label has a unique property signature in the schema,
    so the summary's count for that signature is attributed to it exactly.
    """

    label: str
    min_count: int
    max_count: int
    source_labels: tuple[str, ...]
    property_signature: tuple[str, ...]
    source_target_pairs: tuple[tuple[str, str], ...] = ()

    @property
    def is_exact(self) -> bool:
        return self.min_count == self.max_count

    @property
    def is_range(self) -> bool:
        return self.min_count < self.max_count


@dataclass(frozen=True, slots=True)
class NodeLabelBound:
    """Per-node-label bound on the number of nodes carrying that label.

    ``min_count`` is a **strict lower bound**: nodes from characteristic sets
    that map unambiguously to this label. These nodes are confirmed to carry
    this label by the property-subset + edge-participation tests.

    ``max_count`` is a **strict upper bound**: ``min_count`` plus the total
    node count of every ambiguous group that lists this label as a candidate.
    In the worst case every ambiguous node in every such group could be
    labeled with this label (though the actual joint distribution is
    unknowable from the API alone).

    Note: the max is *not* additive across labels. When two labels share an
    ambiguous group, both list that group's node count in their headroom, so
    ``sum(max_count for all labels) > total_nodes`` is expected. The tight
    fact is ``min_count(A) + max_count(B) ≤ total nodes in {A,B}`` when A and
    B share exactly one ambiguous group of size ``k``, giving ``max_count(A)
    = min_count(A) + k`` and ``max_count(B) = min_count(B) + k``.

    ``ambiguous_group_labels`` records the candidate label sets of every
    ambiguous group that contributed to this label's headroom (deduplicated).
    """

    label: str
    min_count: int
    max_count: int
    exact_structures: int
    ambiguous_group_labels: tuple[tuple[str, ...], ...] = ()

    @property
    def is_exact(self) -> bool:
        return self.min_count == self.max_count

    @property
    def is_range(self) -> bool:
        return self.min_count < self.max_count

    @property
    def ambiguous_headroom(self) -> int:
        return self.max_count - self.min_count


@dataclass(frozen=True, slots=True)
class EdgesByTargetLabel:
    """Total edges targeting a given node label, derived from labelTriples.

    Every edge in Neptune has exactly one source and one target. For each
    node label ``L``, we can sum the edge-label bounds for every edge type
    ``E`` whose ``labelTriples`` mark ``L`` as a target. ``min_count`` is
    the sum of the min bounds, ``max_count`` the sum of the maxes.

    A label with no incoming edge types has ``min == max == 0`` — a
    definitive fact that ``L``-labeled nodes never receive edges.
    """

    label: str
    min_count: int
    max_count: int
    contributing_edge_labels: tuple[str, ...]

    @property
    def is_exact(self) -> bool:
        return self.min_count == self.max_count


@dataclass(frozen=True, slots=True)
class LabelSetCount:
    """Exact count of nodes carrying exactly this set of labels.

    Populated post-scan when the multi-label probe has detected overlaps.
    Each entry represents a distinct labelset partition:

    - Singleton entries (``labels == (L,)``) mean "nodes whose labelset is
      *exactly* ``{L}`` — no other detected labels attached".
    - Multi-label entries (``len(labels) > 1``) mean "nodes carrying every
      label in the set".

    The partition is exhaustive for labels touched by multi-label overlaps:
    every node that has label ``L`` is accounted for by either a singleton
    ``(L,)`` or one or more multi-label entries containing ``L``.
    """

    labels: tuple[str, ...]  # sorted alphabetically
    count: int

    @property
    def is_multi(self) -> bool:
        return len(self.labels) > 1

    def display_name(self) -> str:
        return ":".join(self.labels)


@dataclass(frozen=True, slots=True)
class ConsistencyWarning:
    """A cross-check anomaly between summary aggregates and structure sums.

    Neptune's Graph Summary API reports both per-structure counts *and*
    graph-wide aggregates (``nodeProperties``, ``edgeProperties``,
    ``totalNodePropertyValues``). If the aggregate for property ``P`` does
    not equal the sum of ``structure.count`` for every structure containing
    ``P``, the tool surfaces a warning — commonly caused by truncated
    structure lists on very large graphs.
    """

    kind: str  # "node_property_fill_mismatch", "edge_property_fill_mismatch", etc.
    subject: str  # property name, or "totalNodePropertyValues"
    expected: int  # from summary aggregate
    actual: int  # from summing structure contributions
    detail: str = ""


@dataclass(frozen=True, slots=True)
class PGCorrelationResult:
    """Complete correlation output for a PG summary/schema pair."""

    node_matches: tuple[NodeMatch, ...]
    edge_matches: tuple[EdgeMatch, ...]
    label_stats: dict[str, LabelStats]
    edge_stats: dict[str, EdgeStats]
    ambiguous_node_groups: tuple[AmbiguousNodeGroup, ...]
    unmapped_node_structures: tuple[PGNodeStructure, ...]
    implicit_empty_edges: ImplicitEmptyEdges | None
    """None if there are no residual edges (all edges accounted for by explicit structures)."""

    edge_label_bounds: dict[str, EdgeLabelBound] = field(default_factory=dict)
    """Per-edge-label min/max bounds. Populated for every edge label declared
    in the schema. Exact when ``min == max``, a range otherwise."""

    node_label_bounds: dict[str, NodeLabelBound] = field(default_factory=dict)
    """Per-node-label min/max bounds. Populated for every node label declared
    in the schema. Exact when the label has no ambiguous-group memberships."""

    edges_by_target_label: dict[str, EdgesByTargetLabel] = field(default_factory=dict)
    """Total edges targeting each node label, derived from labelTriples."""

    consistency_warnings: tuple[ConsistencyWarning, ...] = ()
    """Cross-check anomalies between summary aggregates and structure sums."""

    label_partition: tuple[LabelSetCount, ...] = ()
    """Discrete labelset counts. Populated post-scan when the multi-label probe
    detected overlaps AND per-label scans were run. When empty, either no
    multi-labels were found or scans were disabled; consumers should fall
    back to :attr:`node_label_bounds` for per-label counts."""

    label_index: dict[str, LabelSignature] = field(default_factory=dict)

    # ---- Derived aggregates -------------------------------------------------

    @property
    def total_exact_nodes(self) -> int:
        return sum(m.structure.count for m in self.node_matches if m.is_exact)

    @property
    def total_ambiguous_nodes(self) -> int:
        return sum(m.structure.count for m in self.node_matches if m.is_ambiguous)

    @property
    def total_unmapped_nodes(self) -> int:
        return sum(m.structure.count for m in self.node_matches if m.is_unmapped)

    @property
    def total_exact_edges(self) -> int:
        return sum(m.structure.count for m in self.edge_matches if m.is_exact)

    @property
    def total_ambiguous_edges(self) -> int:
        # Explicit ambiguous edge structures plus any ambiguous implicit-empty group.
        explicit = sum(m.structure.count for m in self.edge_matches if m.is_ambiguous)
        implicit = (
            self.implicit_empty_edges.count
            if self.implicit_empty_edges and self.implicit_empty_edges.is_ambiguous
            else 0
        )
        return explicit + implicit
