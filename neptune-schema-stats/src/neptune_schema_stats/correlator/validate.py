"""Consistency checks and post-scan validation for PG correlation results.

Two responsibilities:

- ``_compute_consistency_warnings`` — cross-checks the summary against the
  aggregated per-label stats before scans, surfacing arithmetic mismatches.
- ``validate_scan_totals`` — verifies that post-scan bounds still add up to
  the summary's graph-wide totals, catching drift/inconsistency introduced
  by scans.
"""

from __future__ import annotations

from neptune_schema_stats.correlator.types import (
    ConsistencyWarning,
    PGCorrelationResult,
)
from neptune_schema_stats.models import PGSummary


def _compute_consistency_warnings(*, summary: PGSummary) -> tuple[ConsistencyWarning, ...]:
    """Cross-check the summary's graph-wide aggregates against per-structure sums.

    Currently checks:

    1. ``nodeProperties[P]`` (count of property **values**) versus
       ``sum(structure.count for structures with P in nodeProperties)``
       (count of **nodes** with at least one P value). In Neptune's PG data
       model, vertex properties are multi-valued, so ``value_count ≥ node_count``
       is normal — the excess represents multi-valued properties. We only
       warn when structures claim *more* nodes-with-P than the graph-wide
       value count, which is genuinely impossible in a consistent summary
       (would indicate structure-list corruption).

    2. ``edgeProperties[P]`` must equal ``sum(structure.count for edge
       structures with P in edgeProperties)``. Edge properties are
       single-valued, so any mismatch here is a real inconsistency.

    3. ``totalNodePropertyValues`` should not be *exceeded* by
       ``sum(structure.count * len(structure.node_properties))``. Being
       less than it is normal (multi-valued).
    """
    warnings: list[ConsistencyWarning] = []

    # (1) node property fill sums. Vertex properties are multi-valued in
    # Neptune's PG model: nodeProperties[P] counts VALUES, but our
    # structure-derived sum counts NODES that have at least one value. A
    # small positive delta (values > nodes) is expected and silently
    # accepted. We only warn when the structure-derived NODE count exceeds
    # the graph-wide VALUE count — that's structurally impossible.
    for prop, expected in summary.node_properties.items():
        actual = sum(s.count for s in summary.node_structures if prop in s.node_properties)
        if actual > expected:
            warnings.append(
                ConsistencyWarning(
                    kind="node_property_fill_mismatch",
                    subject=prop,
                    expected=expected,
                    actual=actual,
                    detail=(
                        f"Sum of node-structure counts containing this property "
                        f"({actual:,}) exceeds the summary's value count "
                        f"({expected:,}). This is structurally impossible for a "
                        "consistent summary and may indicate a stale or "
                        "corrupted node-structure list."
                    ),
                )
            )

    # (2) edge property fill sums
    for prop, expected in summary.edge_properties.items():
        actual = sum(s.count for s in summary.edge_structures if prop in s.edge_properties)
        if actual != expected:
            warnings.append(
                ConsistencyWarning(
                    kind="edge_property_fill_mismatch",
                    subject=prop,
                    expected=expected,
                    actual=actual,
                    detail=(
                        "Sum of edge-structure counts containing this property "
                        f"({actual:,}) differs from the summary's aggregate "
                        f"({expected:,}). Structure list may be truncated."
                    ),
                )
            )

    # (3) totalNodePropertyValues sanity check: structures alone should not
    # *exceed* it (that would imply structures with more property-values than
    # the graph actually has). Being less than it is normal (multi-valued).
    if summary.total_node_property_values > 0:
        structure_values = sum(s.count * len(s.node_properties) for s in summary.node_structures)
        if structure_values > summary.total_node_property_values:
            warnings.append(
                ConsistencyWarning(
                    kind="total_node_property_values_exceeded",
                    subject="totalNodePropertyValues",
                    expected=summary.total_node_property_values,
                    actual=structure_values,
                    detail=(
                        f"Sum of structure counts x property-signature width "
                        f"({structure_values:,}) exceeds the summary's "
                        f"totalNodePropertyValues ({summary.total_node_property_values:,}). "
                        "This should not happen for a consistent Neptune summary."
                    ),
                )
            )

    # (4) node structure sum must equal numNodes.
    structure_node_sum = sum(s.count for s in summary.node_structures)
    if structure_node_sum != summary.num_nodes:
        warnings.append(
            ConsistencyWarning(
                kind="node_structure_sum_mismatch",
                subject="numNodes",
                expected=summary.num_nodes,
                actual=structure_node_sum,
                detail=(
                    f"Sum of nodeStructures counts ({structure_node_sum:,}) does not "
                    f"equal numNodes ({summary.num_nodes:,}). Structure list is "
                    "likely truncated or the summary is stale."
                ),
            )
        )

    # (5) edge structure sum must not exceed numEdges — implicit-empty is
    # derived as num_edges - explicit; if the explicit sum alone exceeds
    # num_edges, the summary is inconsistent.
    structure_edge_sum = sum(s.count for s in summary.edge_structures)
    if structure_edge_sum > summary.num_edges:
        warnings.append(
            ConsistencyWarning(
                kind="edge_structure_sum_exceeds_num_edges",
                subject="numEdges",
                expected=summary.num_edges,
                actual=structure_edge_sum,
                detail=(
                    f"Sum of edgeStructures counts ({structure_edge_sum:,}) "
                    f"exceeds numEdges ({summary.num_edges:,}). Summary is "
                    "inconsistent — investigate the Neptune Graph Summary API."
                ),
            )
        )

    return tuple(warnings)


def validate_scan_totals(
    *,
    summary: PGSummary,
    result: PGCorrelationResult,
) -> tuple[ConsistencyWarning, ...]:
    """After a scan has resolved bounds to exacts, verify the sums add up.

    Post-scan invariants:
    - Sum of ``node_label_bounds[L].min_count`` for every exact label equals
      ``num_nodes`` (only checked when *every* label is exact).
    - Sum of ``edge_label_bounds[E].min_count`` for every exact label equals
      ``num_edges`` (only checked when *every* label is exact).

    Both invariants can be violated by:
    - Multi-labeled nodes (a single node counted under multiple labels
      inflates the sum). Typically flagged separately by the multi-label
      probe.
    - Summary staleness between the fetch and the scans
    - A correlator bug or a scan-query returning unexpected values

    Warnings are surfaced without failing the run.
    """
    warnings: list[ConsistencyWarning] = []

    # When the multi-label probe detected overlaps, the partition already
    # counts each node exactly once (exclusive singletons + multi-labelset
    # entries). Use that for the sum check to avoid a false positive when
    # multi-labeled nodes inflate the raw per-label scan sum.
    partition_singleton_labels = {e.labels[0] for e in result.label_partition if not e.is_multi}

    node_bounds = list(result.node_label_bounds.values())
    if node_bounds and all(b.is_exact for b in node_bounds):
        if result.label_partition:
            total = sum(e.count for e in result.label_partition)
            total += sum(
                b.min_count for b in node_bounds if b.label not in partition_singleton_labels
            )
        else:
            total = sum(b.min_count for b in node_bounds)
        if total != summary.num_nodes:
            warnings.append(
                ConsistencyWarning(
                    kind="node_label_sum_mismatch",
                    subject="sum(node label counts)",
                    expected=summary.num_nodes,
                    actual=total,
                    detail=(
                        f"Sum of per-label node counts ({total:,}) does not "
                        f"equal summary numNodes ({summary.num_nodes:,}). "
                        f"Difference: {total - summary.num_nodes:+,}."
                    ),
                )
            )

    edge_bounds = list(result.edge_label_bounds.values())
    if edge_bounds and all(b.is_exact for b in edge_bounds):
        total = sum(b.min_count for b in edge_bounds)
        if total != summary.num_edges:
            warnings.append(
                ConsistencyWarning(
                    kind="edge_label_sum_mismatch",
                    subject="sum(edge label counts)",
                    expected=summary.num_edges,
                    actual=total,
                    detail=(
                        f"Sum of per-edge-label counts ({total:,}) does not "
                        f"equal summary numEdges ({summary.num_edges:,}). "
                        f"Difference: {total - summary.num_edges:+,}."
                    ),
                )
            )

    return tuple(warnings)
