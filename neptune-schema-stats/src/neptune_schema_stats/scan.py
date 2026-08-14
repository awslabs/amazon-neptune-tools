"""Targeted scans to resolve correlation ambiguity.

The API-only correlator produces exact counts for many labels/edges and
bounded ranges for the rest. This module executes minimal openCypher count
queries to collapse ranges into exact values. Scans run by default and are
disabled with ``--api-only`` on the CLI.

Design principles
-----------------

1. **Scan only what's genuinely ambiguous.** Labels and edges whose bounds are
   already exact from correlation (unique property signatures, unambiguous
   structure matches, etc.) are skipped.

2. **Derive the largest candidate.** For each ambiguous group whose members
   sum to a known total (ambiguous node groups, shared-property edge labels),
   we scan all-but-one member and derive the last by subtraction. The
   derived member is chosen to have the largest ``max_count`` — the query we
   skip would have been the most expensive one.

3. **Fail loud, degrade gracefully.** A single query failure aborts the scan
   for that group (with a warning) but the API-only correlation remains
   intact. Users always see something useful.

4. **Purity where possible.** ``plan_scan``, ``apply_scan``, and the query
   helpers are pure; ``execute_scan`` is the only I/O boundary.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, replace
from typing import Any

from neptune_schema_stats.client.base import NeptuneClient, NeptuneClientError
from neptune_schema_stats.client.opencypher import execute_cypher_scalar
from neptune_schema_stats.correlator import (
    EdgeLabelBound,
    LabelSetCount,
    NodeLabelBound,
    PGCorrelationResult,
)
from neptune_schema_stats.models import PGSummary
from neptune_schema_stats.multi_label import MultiLabelProbeResult

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NodeCountScan:
    """One targeted ``MATCH (n:L) RETURN count(n)`` result."""

    label: str
    exact_count: int


@dataclass(frozen=True, slots=True)
class EdgeCountScan:
    """One targeted ``MATCH ()-[r:E]->() RETURN count(r)`` result."""

    label: str
    exact_count: int


@dataclass(frozen=True, slots=True)
class ScanPlan:
    """Plan describing which labels/edges the scan will query.

    ``node_labels_to_query`` and ``edge_labels_to_query`` are the actual set
    of queries the client will issue. Each list is deduplicated.
    """

    node_labels_to_query: tuple[str, ...]
    edge_labels_to_query: tuple[str, ...]

    @property
    def total_queries(self) -> int:
        return len(self.node_labels_to_query) + len(self.edge_labels_to_query)


@dataclass(frozen=True, slots=True)
class ScanResults:
    """Everything produced by executing a :class:`ScanPlan`.

    ``failed_labels`` records any queries that raised (rare — network errors,
    permission issues). The ``apply_scan`` step leaves those labels' bounds
    unchanged and surfaces the failures for reporting.
    """

    plan: ScanPlan
    node_scans: tuple[NodeCountScan, ...] = ()
    edge_scans: tuple[EdgeCountScan, ...] = ()
    failed_node_labels: tuple[str, ...] = ()
    failed_edge_labels: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def plan_scan(result: PGCorrelationResult) -> ScanPlan:
    """Produce a scan plan from a correlation result.

    Every label with a range bound (``min < max``) is added to the query
    list. There is no derivation-by-subtraction — we scan directly for each
    ambiguous label, which is a bit less network-efficient but avoids the
    subtle correctness issues of chaining subtractions across ambiguous
    groups (a peer label participating in more than one group cannot be
    cleanly split).
    """
    node_to_query = _plan_node_queries(result)
    edge_to_query = _plan_edge_queries(result)
    return ScanPlan(
        node_labels_to_query=node_to_query,
        edge_labels_to_query=edge_to_query,
    )


def execute_scan(
    client: NeptuneClient,
    plan: ScanPlan,
    result: PGCorrelationResult,
) -> ScanResults:
    """Execute a scan plan against a live Neptune cluster.

    All queries are issued sequentially. Individual failures are logged and
    recorded in :attr:`ScanResults.failed_node_labels` /
    ``failed_edge_labels`` but do not abort the scan as a whole.
    """
    _ = result  # unused: kept for backwards-compatible signature
    node_scans: list[NodeCountScan] = []
    edge_scans: list[EdgeCountScan] = []
    failed_nodes: list[str] = []
    failed_edges: list[str] = []

    for label in plan.node_labels_to_query:
        try:
            c = query_node_label_count(client, label)
            node_scans.append(NodeCountScan(label=label, exact_count=c))
        except NeptuneClientError as exc:
            log.warning("Node count scan failed for %s: %s", label, exc)
            failed_nodes.append(label)

    for label in plan.edge_labels_to_query:
        try:
            c = query_edge_label_count(client, label)
            edge_scans.append(EdgeCountScan(label=label, exact_count=c))
        except NeptuneClientError as exc:
            log.warning("Edge count scan failed for %s: %s", label, exc)
            failed_edges.append(label)

    return ScanResults(
        plan=plan,
        node_scans=tuple(node_scans),
        edge_scans=tuple(edge_scans),
        failed_node_labels=tuple(failed_nodes),
        failed_edge_labels=tuple(failed_edges),
    )


def apply_scan(
    result: PGCorrelationResult,
    scan: ScanResults,
    *,
    summary: PGSummary | None = None,
    multi_label: MultiLabelProbeResult | None = None,
) -> PGCorrelationResult:
    """Return a new correlation result with bounds collapsed to exact.

    For every scanned or derived label/edge, ``min_count`` and ``max_count``
    are set to the scanned/derived value. Bounds for failed queries are left
    untouched.

    :param summary: optional PG summary; when supplied, post-scan sum
        validations (``sum(node counts) == num_nodes``, ``sum(edge counts)
        == num_edges``) run and any discrepancies are appended to the
        result's consistency warnings.
    :param multi_label: optional multi-label probe result. When present with
        detected overlaps, the returned result includes a ``label_partition``
        that splits affected labels into exclusive counts and multi-labelset
        counts. Also relaxes the "scanned count exceeds max" sanity check
        for labels involved in overlaps (the excess is expected —
        ``MATCH (n:L) RETURN count(n)`` counts multi-labeled nodes too).
    """
    # Precompute overlap totals per label (used both to relax the sanity
    # floor and to compute the partition below).
    overlap_by_label: dict[str, int] = defaultdict(int)
    pair_counts: dict[frozenset[str], int] = {}
    if multi_label is not None:
        for pair in multi_label.hits:
            pair_counts[frozenset(pair.labels)] = pair.node_count
            for label in pair.labels:
                overlap_by_label[label] += pair.node_count
    scanned_nodes = {s.label: s.exact_count for s in scan.node_scans}
    scanned_edges = {s.label: s.exact_count for s in scan.edge_scans}

    # Sanity: any scanned value that is negative or exceeds the pre-scan max
    # is treated as untrustworthy and dropped. Bounds fall back to the
    # correlator's original range. This most often indicates multi-labeled
    # nodes inflating a MATCH (n:L) RETURN count(n) result — e.g. a node
    # labeled both :Artist and :movie is counted by both queries. When we
    # have multi-label probe data, we know the expected overlap and can
    # accept scans that exceed max_count by no more than the overlap
    # contribution.
    rejected_scanned_nodes: list[str] = []
    for label, count in list(scanned_nodes.items()):
        bound = result.node_label_bounds.get(label)
        if bound is None:
            continue
        allowed_max = bound.max_count + overlap_by_label.get(label, 0)
        if count < 0 or count > allowed_max:
            rejected_scanned_nodes.append(label)
            del scanned_nodes[label]
            log.warning(
                "Rejected scanned node count for '%s' (value=%s, bound=[%s, %s], "
                "multi-label overlap=%s). Keeping original range bound.",
                label,
                count,
                bound.min_count,
                bound.max_count,
                overlap_by_label.get(label, 0),
            )

    rejected_scanned_edges: list[str] = []
    for label, count in list(scanned_edges.items()):
        bound = result.edge_label_bounds.get(label)
        if bound is None:
            continue
        if count < 0 or count > bound.max_count:
            rejected_scanned_edges.append(label)
            del scanned_edges[label]
            log.warning(
                "Rejected scanned edge count for '%s' (value=%s, bound=[%s, %s]). "
                "Keeping original range bound.",
                label,
                count,
                bound.min_count,
                bound.max_count,
            )

    new_node_bounds: dict[str, NodeLabelBound] = {}
    for label, bound in result.node_label_bounds.items():
        if label in scanned_nodes:
            c = scanned_nodes[label]
            new_node_bounds[label] = replace(bound, min_count=c, max_count=c)
        else:
            new_node_bounds[label] = bound

    new_edge_bounds: dict[str, EdgeLabelBound] = {}
    for label, bound in result.edge_label_bounds.items():
        if label in scanned_edges:
            c = scanned_edges[label]
            new_edge_bounds[label] = replace(bound, min_count=c, max_count=c)
        else:
            new_edge_bounds[label] = bound

    # Recompute edges-by-target given the new edge bounds.
    from neptune_schema_stats.correlator import EdgesByTargetLabel  # local import to avoid cycles

    new_edges_by_target: dict[str, EdgesByTargetLabel] = {}
    for label, prev in result.edges_by_target_label.items():
        if not prev.contributing_edge_labels:
            new_edges_by_target[label] = prev
            continue
        total_min = 0
        total_max = 0
        for et in prev.contributing_edge_labels:
            eb = new_edge_bounds.get(et)
            if eb is None:
                continue
            total_min += eb.min_count
            total_max += eb.max_count
        new_edges_by_target[label] = EdgesByTargetLabel(
            label=label,
            min_count=total_min,
            max_count=total_max,
            contributing_edge_labels=prev.contributing_edge_labels,
        )

    # Post-scan, redistribute property fills from ambiguous structures whose
    # split is now known via the scan (single-structure groups only — exact
    # arithmetic).
    new_label_stats = _redistribute_ambiguous_fills(
        result=result,
        new_node_bounds=new_node_bounds,
    )

    updated = replace(
        result,
        node_label_bounds=new_node_bounds,
        edge_label_bounds=new_edge_bounds,
        edges_by_target_label=new_edges_by_target,
        label_stats=new_label_stats,
    )

    # Compute the label partition when the multi-label probe found overlaps.
    # For each affected label L, exclusive count = scan(L) - sum(pair counts
    # containing L). Emits both singleton entries (exclusive) and pair entries
    # (multi-labeled). Any negatives indicate 3+ way overlaps that the pairwise
    # probe cannot detect — surface those as consistency warnings.
    from neptune_schema_stats.correlator import ConsistencyWarning  # avoid cycles

    partition_warnings: list[ConsistencyWarning] = []
    if multi_label is not None and multi_label.any_multi_label:
        partition_entries: list[LabelSetCount] = []
        for label, overlap in overlap_by_label.items():
            bound = new_node_bounds.get(label)
            if bound is None or not bound.is_exact:
                # Can't compute exclusive without a trustworthy scan total.
                continue
            exclusive = bound.min_count - overlap
            if exclusive < 0:
                partition_warnings.append(
                    ConsistencyWarning(
                        kind="label_partition_negative_exclusive",
                        subject=label,
                        expected=overlap,
                        actual=bound.min_count,
                        detail=(
                            f"exclusive count for '{label}' would be {exclusive}. "
                            "This typically means 3+ way label overlaps exist that the "
                            "pairwise multi-label probe cannot detect."
                        ),
                    )
                )
                continue
            partition_entries.append(LabelSetCount(labels=(label,), count=exclusive))
        for label_set, count in pair_counts.items():
            partition_entries.append(LabelSetCount(labels=tuple(sorted(label_set)), count=count))
        # Sort: singletons first (by label), then multi-labelsets (by first label).
        partition_entries.sort(key=lambda e: (len(e.labels) > 1, e.labels))
        if partition_entries:
            updated = replace(updated, label_partition=tuple(partition_entries))

    # Surface rejected scans as consistency warnings so they show up in the
    # report (and JSON output) rather than only in logs.
    rejection_warnings: list[ConsistencyWarning] = []
    for label in rejected_scanned_nodes:
        b = result.node_label_bounds.get(label)
        actual = (
            scan.node_scans
            and next((s.exact_count for s in scan.node_scans if s.label == label), 0)
        ) or 0
        rejection_warnings.append(
            ConsistencyWarning(
                kind="rejected_scanned_node_count",
                subject=label,
                expected=(b.max_count if b else 0),
                actual=actual,
                detail=(
                    "scanned count exceeded pre-scan max — likely multi-labeled overlap. "
                    "Original bound kept."
                ),
            )
        )
    for label in rejected_scanned_edges:
        b = result.edge_label_bounds.get(label)
        actual = (
            scan.edge_scans
            and next((s.exact_count for s in scan.edge_scans if s.label == label), 0)
        ) or 0
        rejection_warnings.append(
            ConsistencyWarning(
                kind="rejected_scanned_edge_count",
                subject=label,
                expected=(b.max_count if b else 0),
                actual=actual,
                detail="scanned count exceeded pre-scan max. Original bound kept.",
            )
        )

    if rejection_warnings or partition_warnings:
        updated = replace(
            updated,
            consistency_warnings=(
                updated.consistency_warnings + tuple(rejection_warnings) + tuple(partition_warnings)
            ),
        )

    # Post-scan sum validation.
    if summary is not None:
        from neptune_schema_stats.correlator import validate_scan_totals

        extra_warnings = validate_scan_totals(summary=summary, result=updated)
        if extra_warnings:
            updated = replace(
                updated,
                consistency_warnings=updated.consistency_warnings + extra_warnings,
            )

    return updated


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def query_node_label_count(client: NeptuneClient, label: str) -> int:
    """Run ``MATCH (n:<label>) RETURN count(n)`` and return the count."""
    query = f"MATCH (n:{_quote_label(label)}) RETURN count(n) AS c"
    return execute_cypher_scalar(client, query)


def query_edge_label_count(client: NeptuneClient, label: str) -> int:
    """Run ``MATCH ()-[r:<label>]->() RETURN count(r)`` and return the count."""
    query = f"MATCH ()-[r:{_quote_label(label)}]->() RETURN count(r) AS c"
    return execute_cypher_scalar(client, query)


# ---------------------------------------------------------------------------
# Plan internals
# ---------------------------------------------------------------------------


def _plan_node_queries(
    result: PGCorrelationResult,
) -> tuple[str, ...]:
    """Return every node label with a range-bounded count. Each will be
    scanned directly via ``MATCH (n:L) RETURN count(n)``."""
    labels = {label for label, bound in result.node_label_bounds.items() if bound.is_range}
    return tuple(sorted(labels))


def _plan_edge_queries(
    result: PGCorrelationResult,
) -> tuple[str, ...]:
    """Return every edge label with a range-bounded count. Each will be
    scanned directly via ``MATCH ()-[r:L]->() RETURN count(r)``."""
    labels = {label for label, bound in result.edge_label_bounds.items() if bound.is_range}
    return tuple(sorted(labels))


# ---------------------------------------------------------------------------
# Property fill redistribution
# ---------------------------------------------------------------------------


def _redistribute_ambiguous_fills(
    *,
    result: PGCorrelationResult,
    new_node_bounds: dict[str, NodeLabelBound],
) -> dict[str, Any]:
    """Return a new ``label_stats`` mapping with property fill counts updated
    for labels that absorbed ambiguous-group contributions post-scan.

    Handled precisely:
    - Single-structure ambiguous groups where each candidate label
      participates in *only that group*: the label's contribution from the
      group equals its post-scan count minus its pre-scan exact-attributed
      count. That contribution's property fills come from the single
      structure's property signature.

    Left unchanged:
    - Multi-structure ambiguous groups (splitting fills across multiple
      structures requires an assumption we don't want to make).
    - Labels that participate in more than one ambiguous group. Attributing
      "how much of this label's contribution came from THIS group" isn't
      possible without additional queries, so we leave their fills at the
      pre-scan values rather than double-count. (Their bounds/count are
      still resolved to exact; only the property-fill percentages are
      conservative.)
    - Labels that had no ambiguous headroom (their fills are already exact).
    """
    from neptune_schema_stats.correlator import LabelStats

    # Count each label's ambiguous-group memberships so we can skip
    # multi-group labels (see docstring for why).
    membership_count: dict[str, int] = defaultdict(int)
    for group in result.ambiguous_node_groups:
        for candidate in group.candidate_labels:
            membership_count[candidate] += 1

    fill_deltas: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for group in result.ambiguous_node_groups:
        if len(group.structures) != 1:
            continue
        structure = group.structures[0]
        for label in group.candidate_labels:
            if membership_count.get(label, 0) > 1:
                # Multi-group label — can't cleanly attribute contribution.
                continue
            prev = result.label_stats.get(label)
            if prev is None:
                continue
            new_bound = new_node_bounds.get(label)
            if new_bound is None or not new_bound.is_exact:
                continue
            contribution = new_bound.min_count - prev.node_count
            if contribution <= 0:
                continue
            for prop in structure.node_properties:
                fill_deltas[label][prop] += contribution

    new_stats: dict[str, LabelStats] = {}
    for label, prev in result.label_stats.items():
        new_bound = new_node_bounds.get(label)
        new_count = (
            new_bound.min_count
            if (new_bound is not None and new_bound.is_exact)
            else prev.node_count
        )
        deltas = fill_deltas.get(label, {})
        if not deltas:
            new_stats[label] = replace(prev, node_count=new_count)
            continue
        new_fills = {
            prop: prev.property_fill_counts.get(prop, 0) + deltas.get(prop, 0)
            for prop in set(prev.property_fill_counts) | set(deltas)
        }
        new_stats[label] = replace(
            prev,
            node_count=new_count,
            property_fill_counts=new_fills,
        )
    return new_stats


# ---------------------------------------------------------------------------
# Label quoting
# ---------------------------------------------------------------------------


def _quote_label(label: str) -> str:
    """Backtick-quote a label; embedded backticks are doubled per openCypher."""
    escaped = label.replace("`", "``")
    return f"`{escaped}`"
