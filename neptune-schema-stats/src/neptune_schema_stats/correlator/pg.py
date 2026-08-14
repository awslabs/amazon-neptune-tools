"""Property-graph correlator — the main correlation logic.

Combines Neptune's Graph Summary characteristic sets with the PG Schema's
label signatures to produce per-label statistics. See
:mod:`neptune_schema_stats.correlator.types` for the data structures this
module produces.

Correlation strategy — API-only tier
------------------------------------

Two discriminators, applied in order:

1. **Property subset test.** A node characteristic set ``S`` is compatible with
   a label ``L`` iff ``S.properties ⊆ L.properties``. Any property present in
   the structure but absent from the label's schema disqualifies that label.
   The converse — properties present in the label but absent from the
   structure — is *not* disqualifying because Neptune's property graph does
   not require all label properties to be populated.

2. **Edge participation test.** If the structure ``S`` has an outgoing edge
   type ``E``, then a candidate label ``L`` must be a valid source for ``E``
   in the schema's ``labelTriples``. This eliminates labels that grammatically
   cannot emit edges of that type. Absence of outgoing edges in the structure
   is not disqualifying (a label may permit outgoing edges without every node
   using them).

After applying both discriminators, each structure has:

- **0 candidates** → *unmapped*. Usually means the schema is stale relative to
  the summary (new labels/properties in the graph that haven't been re-scanned).
- **1 candidate** → *exact* match; the structure's count is attributed to that
  label with full confidence.
- **>1 candidates** → *ambiguous*. Structures with identical property/edge
  signatures across multiple labels (e.g. air-routes' ``continent``/``country``)
  cannot be split without a full-graph scan. The correlator groups these
  structures into :class:`AmbiguousNodeGroup` records; per-label scans in
  :mod:`neptune_schema_stats.scan` resolve them to exact counts on the
  default run (disabled with ``--api-only``).

Edge correlation uses an exact-signature match against ``edgeLabelDetails``.
The Graph Summary API sometimes elides an "implicit empty" edge structure
(edges with no properties); we recover its count via subtraction
(``num_edges - sum(explicit)``) and attribute it to edge labels whose schema
declares zero properties. If more than one such label exists, the group is
ambiguous (see the identity-graph fixture for a canonical example).
"""

from __future__ import annotations

from collections import defaultdict

from neptune_schema_stats.correlator.types import (
    AmbiguousNodeGroup,
    EdgeLabelBound,
    EdgeMatch,
    EdgesByTargetLabel,
    EdgeStats,
    ImplicitEmptyEdges,
    LabelSignature,
    LabelStats,
    NodeLabelBound,
    NodeMatch,
    PGCorrelationResult,
)
from neptune_schema_stats.correlator.validate import _compute_consistency_warnings
from neptune_schema_stats.models import PGEdgeStructure, PGNodeStructure, PGSchema, PGSummary


def build_label_index(schema: PGSchema) -> dict[str, LabelSignature]:
    """Build a per-label signature index from a completed PG schema.

    Returns an empty dict if the schema is not usable (state != Completed).
    """
    if not schema.is_usable():
        return {}

    outgoing: dict[str, set[str]] = defaultdict(set)
    incoming: dict[str, set[str]] = defaultdict(set)
    for triple in schema.label_triples:
        outgoing[triple.from_label].add(triple.edge_type)
        incoming[triple.to_label].add(triple.edge_type)

    return {
        label: LabelSignature(
            label=label,
            properties=frozenset(props.keys()),
            valid_outgoing=frozenset(outgoing.get(label, ())),
            valid_incoming=frozenset(incoming.get(label, ())),
        )
        for label, props in schema.node_label_details.items()
    }


def match_structure_to_labels(
    structure: PGNodeStructure,
    label_index: dict[str, LabelSignature],
) -> tuple[str, ...]:
    """Return the sorted tuple of labels compatible with ``structure``.

    Applies both discriminators (property subset + edge participation). An
    empty tuple means no label matches (unmapped); a single-element tuple
    means an exact match; multiple elements mean ambiguous.
    """
    struct_props = frozenset(structure.node_properties)
    struct_outgoing = frozenset(structure.distinct_outgoing_edge_labels)

    candidates: list[str] = []
    for label, sig in label_index.items():
        if not struct_props.issubset(sig.properties):
            continue
        if not struct_outgoing.issubset(sig.valid_outgoing):
            continue
        candidates.append(label)

    return tuple(sorted(candidates))


def match_edge_structure(
    structure: PGEdgeStructure,
    schema: PGSchema,
) -> tuple[str, ...]:
    """Return the sorted tuple of edge labels whose schema-declared property signature
    exactly matches this edge structure's property set.
    """
    struct_props = frozenset(structure.edge_properties)
    candidates: list[str] = []
    for label, props in schema.edge_label_details.items():
        if frozenset(props.keys()) == struct_props:
            candidates.append(label)
    return tuple(sorted(candidates))


def correlate_pg(summary: PGSummary, schema: PGSchema) -> PGCorrelationResult:
    """Correlate a PG summary against a PG schema.

    :raises ValueError: if the schema is not in a usable (``Completed``) state.
    """
    if not schema.is_usable():
        raise ValueError(
            "PG schema is not usable — state is "
            f"{schema.state().value}. Run --compute-schema --wait-for-schema first."
        )

    label_index = build_label_index(schema)

    # ---- Node correlation ---------------------------------------------------
    node_matches: list[NodeMatch] = []
    for structure in summary.node_structures:
        candidates = match_structure_to_labels(structure, label_index)
        node_matches.append(NodeMatch(structure=structure, candidate_labels=candidates))

    label_stats = _aggregate_label_stats(node_matches, label_index)
    ambiguous_groups = _group_ambiguous_matches(node_matches)
    unmapped = tuple(m.structure for m in node_matches if m.is_unmapped)

    # ---- Edge correlation ---------------------------------------------------
    edge_matches: list[EdgeMatch] = []
    for structure in summary.edge_structures:
        candidates = match_edge_structure(structure, schema)
        edge_matches.append(EdgeMatch(structure=structure, candidate_labels=candidates))

    edge_stats = _aggregate_edge_stats(edge_matches)
    implicit_empty = _compute_implicit_empty_edges(summary, schema, edge_matches)
    edge_label_bounds = _compute_edge_label_bounds(
        summary=summary,
        schema=schema,
        node_matches=node_matches,
        edge_matches=edge_matches,
        edge_stats=edge_stats,
        implicit_empty=implicit_empty,
    )

    # ---- Derived views ------------------------------------------------------
    node_label_bounds = _compute_node_label_bounds(
        label_stats=label_stats,
        ambiguous_groups=ambiguous_groups,
        label_index=label_index,
    )
    edges_by_target = _compute_edges_by_target(
        edge_label_bounds=edge_label_bounds,
        schema=schema,
        label_index=label_index,
    )
    consistency_warnings = _compute_consistency_warnings(summary=summary)

    return PGCorrelationResult(
        node_matches=tuple(node_matches),
        edge_matches=tuple(edge_matches),
        label_stats=label_stats,
        edge_stats=edge_stats,
        ambiguous_node_groups=ambiguous_groups,
        unmapped_node_structures=unmapped,
        implicit_empty_edges=implicit_empty,
        edge_label_bounds=edge_label_bounds,
        node_label_bounds=node_label_bounds,
        edges_by_target_label=edges_by_target,
        consistency_warnings=consistency_warnings,
        label_index=label_index,
    )


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _aggregate_label_stats(
    matches: list[NodeMatch],
    label_index: dict[str, LabelSignature],
) -> dict[str, LabelStats]:
    """Aggregate per-label metrics from exact matches only.

    Ambiguous structures deliberately do not contribute to per-label counts —
    doing so would silently double-count nodes. Callers can inspect the
    :class:`AmbiguousNodeGroup` records for the ambiguous residual.
    """
    counts: dict[str, int] = defaultdict(int)
    contributors: dict[str, int] = defaultdict(int)
    fill: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for m in matches:
        if not m.is_exact:
            continue
        label = m.candidate_labels[0]
        counts[label] += m.structure.count
        contributors[label] += 1
        for prop in m.structure.node_properties:
            fill[label][prop] += m.structure.count

    result: dict[str, LabelStats] = {}
    for label, sig in label_index.items():
        # Always include every label in the schema so downstream reports can
        # show 0-count labels (indicates the label was declared but no nodes
        # matched via exact correlation).
        # Ensure every declared property has an entry (0 if never populated).
        prop_fill = {prop: fill[label].get(prop, 0) for prop in sorted(sig.properties)}
        result[label] = LabelStats(
            label=label,
            node_count=counts.get(label, 0),
            contributing_structures=contributors.get(label, 0),
            property_fill_counts=prop_fill,
        )
    return result


def _group_ambiguous_matches(matches: list[NodeMatch]) -> tuple[AmbiguousNodeGroup, ...]:
    """Group ambiguous structures by their candidate label set."""
    buckets: dict[tuple[str, ...], list[PGNodeStructure]] = defaultdict(list)
    for m in matches:
        if m.is_ambiguous:
            buckets[m.candidate_labels].append(m.structure)

    groups = [
        AmbiguousNodeGroup(
            candidate_labels=labels,
            structures=tuple(structs),
            total_count=sum(s.count for s in structs),
        )
        for labels, structs in buckets.items()
    ]
    # Sort by total_count descending so the most impactful ambiguity surfaces first.
    groups.sort(key=lambda g: g.total_count, reverse=True)
    return tuple(groups)


def _aggregate_edge_stats(matches: list[EdgeMatch]) -> dict[str, EdgeStats]:
    """Aggregate edge counts from exact-signature edge matches."""
    counts: dict[str, int] = defaultdict(int)
    sig_map: dict[str, tuple[str, ...]] = {}
    for m in matches:
        if not m.is_exact:
            continue
        label = m.candidate_labels[0]
        counts[label] += m.structure.count
        sig_map[label] = m.structure.edge_properties

    return {
        label: EdgeStats(
            label=label,
            edge_count=count,
            property_signature=sig_map.get(label, ()),
        )
        for label, count in counts.items()
    }


def _compute_implicit_empty_edges(
    summary: PGSummary,
    schema: PGSchema,
    matches: list[EdgeMatch],
) -> ImplicitEmptyEdges | None:
    """Compute the residual edge count and attribute it to zero-property edge labels.

    If the explicit edge structures already sum to ``num_edges`` (or exceed it,
    which shouldn't happen but is defensively handled), return ``None`` —
    there is no implicit-empty residual.
    """
    accounted = sum(m.structure.count for m in matches)
    residual = summary.num_edges - accounted
    if residual <= 0:
        return None

    zero_prop_labels = tuple(
        sorted(label for label, props in schema.edge_label_details.items() if not props)
    )
    return ImplicitEmptyEdges(count=residual, candidate_labels=zero_prop_labels)


def _compute_edge_label_bounds(
    *,
    summary: PGSummary,
    schema: PGSchema,
    node_matches: list[NodeMatch],
    edge_matches: list[EdgeMatch],
    edge_stats: dict[str, EdgeStats],
    implicit_empty: ImplicitEmptyEdges | None,
) -> dict[str, EdgeLabelBound]:
    """For every edge label in the schema, compute a (min, max) bound on its
    total edge count in the graph.

    See :class:`EdgeLabelBound` for the derivation rules.
    """
    # Step 1: per-edge-type minimums from node structures.
    #
    # For each node structure, every node in it emits at least one outgoing
    # edge of every type in ``distinct_outgoing_edge_labels``. That structure
    # therefore contributes ``count`` to the *total* minimum for each such
    # edge type — regardless of node-label ambiguity, since the structural
    # fact ("has at least one outgoing E edge") holds independent of which
    # candidate label a node actually carries.
    edge_type_min: dict[str, int] = defaultdict(int)
    for m in node_matches:
        for edge_type in m.structure.distinct_outgoing_edge_labels:
            edge_type_min[edge_type] += m.structure.count

    # Step 2: source labels + source-target pairs per edge type from labelTriples.
    edge_type_sources: dict[str, list[str]] = defaultdict(list)
    edge_type_pairs: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for triple in schema.label_triples:
        srcs = edge_type_sources[triple.edge_type]
        if triple.from_label not in srcs:
            srcs.append(triple.from_label)
        pair = (triple.from_label, triple.to_label)
        if pair not in edge_type_pairs[triple.edge_type]:
            edge_type_pairs[triple.edge_type].append(pair)

    # Step 3: group edge labels by shared property signature to identify which
    # labels compete for the same edge structure (or the implicit-empty pool).
    signature_groups: dict[frozenset[str], list[str]] = defaultdict(list)
    for label, props in schema.edge_label_details.items():
        signature_groups[frozenset(props.keys())].append(label)

    # Step 4: total count observed for each property signature.
    signature_totals: dict[frozenset[str], int] = {}
    for edge_match in edge_matches:
        sig = frozenset(edge_match.structure.edge_properties)
        signature_totals[sig] = signature_totals.get(sig, 0) + edge_match.structure.count
    if implicit_empty is not None:
        signature_totals.setdefault(frozenset(), 0)
        signature_totals[frozenset()] += implicit_empty.count

    # Step 5: compute per-label bound.
    bounds: dict[str, EdgeLabelBound] = {}
    for label, props in schema.edge_label_details.items():
        sig = frozenset(props.keys())
        siblings = signature_groups[sig]
        total_for_sig = signature_totals.get(sig, 0)
        floor = edge_type_min.get(label, 0)

        if len(siblings) == 1:
            # Unique property signature — exact count from summary.
            min_count = max_count = total_for_sig
            # Sanity: node-structure floor must be <= observed total.
            if floor > total_for_sig:
                # Data inconsistency: floor exceeds observed. Fall back to floor as min.
                min_count = floor
                max_count = max(total_for_sig, floor)
        else:
            # Shared signature — bound by residual arithmetic.
            other_floors = sum(edge_type_min.get(other, 0) for other in siblings if other != label)
            min_count = floor
            max_count = max(min_count, total_for_sig - other_floors)

        bounds[label] = EdgeLabelBound(
            label=label,
            min_count=min_count,
            max_count=max_count,
            source_labels=tuple(sorted(edge_type_sources.get(label, ()))),
            property_signature=tuple(sorted(props.keys())),
            source_target_pairs=tuple(sorted(edge_type_pairs.get(label, ()))),
        )
    return bounds


# ---------------------------------------------------------------------------
# JSON helper (for --dump-style output in CLI)
# ---------------------------------------------------------------------------


def _compute_node_label_bounds(
    *,
    label_stats: dict[str, LabelStats],
    ambiguous_groups: tuple[AmbiguousNodeGroup, ...],
    label_index: dict[str, LabelSignature],
) -> dict[str, NodeLabelBound]:
    """For every node label, compute (min, max) node counts.

    - ``min_count`` = nodes attributed via exact structure matches (already in
      ``label_stats``).
    - ``max_count`` = ``min_count`` plus the total nodes in every ambiguous
      group where this label is a candidate.

    The max is deliberately not additive across labels: two labels sharing an
    ambiguous group each list its size as headroom, so overall
    ``sum(max) > total_nodes`` is expected and correct. What is invariant is
    each label's *own* count falling within its ``[min, max]``.
    """
    bounds: dict[str, NodeLabelBound] = {}
    for label, sig in label_index.items():
        stats = label_stats.get(label)
        min_count = stats.node_count if stats else 0
        exact_structs = stats.contributing_structures if stats else 0
        ambig_lists: list[tuple[str, ...]] = []
        headroom = 0
        for group in ambiguous_groups:
            if label in group.candidate_labels:
                headroom += group.total_count
                ambig_lists.append(group.candidate_labels)
        bounds[label] = NodeLabelBound(
            label=label,
            min_count=min_count,
            max_count=min_count + headroom,
            exact_structures=exact_structs,
            ambiguous_group_labels=tuple(ambig_lists),
        )
        # touch sig to satisfy linters; it's a schema pointer the caller keeps.
        _ = sig
    return bounds


def _compute_edges_by_target(
    *,
    edge_label_bounds: dict[str, EdgeLabelBound],
    schema: PGSchema,
    label_index: dict[str, LabelSignature],
) -> dict[str, EdgesByTargetLabel]:
    """For every node label, sum the bounded edge counts across every edge
    type whose ``labelTriples`` name it as target."""
    # Build target-label -> list of edge_types map from labelTriples.
    target_map: dict[str, list[str]] = defaultdict(list)
    for triple in schema.label_triples:
        # Only include an edge_type once per target — labelTriples repeats
        # per source/target combination, but the edge_type count is one total.
        edge_types = target_map[triple.to_label]
        if triple.edge_type not in edge_types:
            edge_types.append(triple.edge_type)

    result: dict[str, EdgesByTargetLabel] = {}
    for label in label_index:
        edge_types = tuple(sorted(target_map.get(label, [])))
        total_min = 0
        total_max = 0
        for et in edge_types:
            bound = edge_label_bounds.get(et)
            if bound is None:
                continue
            total_min += bound.min_count
            total_max += bound.max_count
        result[label] = EdgesByTargetLabel(
            label=label,
            min_count=total_min,
            max_count=total_max,
            contributing_edge_labels=edge_types,
        )
    return result
