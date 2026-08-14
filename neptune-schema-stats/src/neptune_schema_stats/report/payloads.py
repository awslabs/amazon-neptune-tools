"""JSON payload builders — the machine-readable equivalent of the concise text reports.

Every payload mirrors the shape of the text report at the concise level.
Consumers that need raw internal state use ``--dump`` instead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from neptune_schema_stats.client.statistics import StatisticsInfo
    from neptune_schema_stats.correlator import PGCorrelationResult
    from neptune_schema_stats.correlator.rdf import RDFCorrelationResult
    from neptune_schema_stats.fallback.pg import FallbackResult
    from neptune_schema_stats.fallback.rdf import RDFFallbackResult
    from neptune_schema_stats.models import PGSchema, PGSummary, RDFSummary
    from neptune_schema_stats.multi_label import MultiLabelProbeResult
    from neptune_schema_stats.scan import ScanResults

from neptune_schema_stats.report._shared import statistics_payload as _statistics_payload


def pg_report_payload(
    summary: PGSummary,
    schema: PGSchema,
    result: PGCorrelationResult,
    *,
    endpoint: str | None = None,
    multi_label: MultiLabelProbeResult | None = None,
    scan: ScanResults | None = None,
    statistics: StatisticsInfo | None = None,
) -> dict[str, object]:
    """Return the concise PG report as a JSON-serializable dict.

    The shape mirrors :func:`render_pg_report` at the concise level. Each
    label appears once with its count (or count + max_count if still a
    range) and the ordered list of properties declared for it in the schema.
    Empty sections are omitted so consumers can quickly branch on presence.
    """
    payload: dict[str, object] = {
        "endpoint": endpoint or "",
        "mode": "pg",
        "totals": {"nodes": summary.num_nodes, "edges": summary.num_edges},
    }

    # Node labels: ordered like the text report (descending max_count, then label).
    node_labels: list[dict[str, object]] = []
    for bound in sorted(
        result.node_label_bounds.values(),
        key=lambda b: (-b.max_count, b.label),
    ):
        entry: dict[str, object] = {"label": bound.label, "count": bound.min_count}
        if bound.max_count != bound.min_count:
            entry["max_count"] = bound.max_count
        stats = result.label_stats.get(bound.label)
        if stats and stats.property_fill_counts:
            entry["properties"] = list(stats.property_fill_counts.keys())
        node_labels.append(entry)
    if node_labels:
        payload["node_labels"] = node_labels

    # Edge labels: ordered like the text report.
    edge_labels: list[dict[str, object]] = []
    for bound in sorted(
        result.edge_label_bounds.values(),
        key=lambda b: (-b.max_count, b.label),
    ):
        entry = {"label": bound.label, "count": bound.min_count}
        if bound.max_count != bound.min_count:
            entry["max_count"] = bound.max_count
        if bound.source_target_pairs:
            entry["source_target_pairs"] = [list(p) for p in sorted(set(bound.source_target_pairs))]
        elif bound.source_labels:
            entry["sources"] = list(bound.source_labels)
        if bound.is_exact:
            mps = _mean_per_source_numeric(result, bound.label)
            if mps is not None:
                entry["mean_per_source"] = mps
        if bound.property_signature:
            entry["properties"] = list(bound.property_signature)
        edge_labels.append(entry)
    if edge_labels:
        payload["edge_labels"] = edge_labels

    # Multi-label combinations (only when the partition actually has overlaps).
    combos: list[dict[str, object]] = []
    for entry in result.label_partition:
        if entry.is_multi:
            combos.append({"labels": list(entry.labels), "count": entry.count})
    if combos:
        payload["multi_label_combinations"] = combos

    # Consistency warnings (only when present).
    if result.consistency_warnings:
        payload["warnings"] = [
            {
                "kind": w.kind,
                "subject": w.subject,
                "detail": w.detail,
            }
            for w in result.consistency_warnings
        ]

    # Statistics metadata (only when available).
    if statistics is not None:
        payload["statistics"] = _statistics_payload(statistics)

    return payload


def rdf_report_payload(
    summary: RDFSummary,
    result: RDFCorrelationResult,
    *,
    endpoint: str | None = None,
    statistics: StatisticsInfo | None = None,
) -> dict[str, object]:
    """Return the concise RDF report as a JSON-serializable dict.

    Structure mirrors :func:`render_rdf_report` at the concise level:
    top-line totals, subject-typing split, predicate list, and (when
    available) declared-class counts.
    """
    payload: dict[str, object] = {
        "endpoint": endpoint or "",
        "mode": "rdf",
        "totals": {
            "distinct_subjects": summary.num_distinct_subjects,
            "distinct_predicates": summary.num_distinct_predicates,
            "quads": summary.num_quads,
            "declared_classes": len(summary.classes),
        },
        "subject_typing": {
            "typed": result.num_typed_subjects,
            "untyped": result.num_untyped_subjects,
        },
    }

    if result.predicate_stats:
        payload["predicates"] = [
            {
                "local_name": ps.local_name,
                "uri": ps.uri,
                "occurrences": ps.occurrence_count,
            }
            for ps in result.predicate_stats
        ]

    if result.class_counts:
        payload["classes"] = [
            {"class_uri": cc.class_uri, "subject_count": cc.subject_count}
            for cc in result.class_counts
        ]

    if statistics is not None:
        payload["statistics"] = _statistics_payload(statistics)

    return payload


def _mean_per_source_numeric(result: PGCorrelationResult, edge_label: str) -> float | None:
    """Return the mean edges-per-source as a float, or ``None`` if either the
    edge or any source label is still a range. Same contract as
    :func:`_mean_per_source` but returns a raw number instead of a display
    string.
    """
    edge_bound = result.edge_label_bounds.get(edge_label)
    if edge_bound is None or not edge_bound.is_exact:
        return None
    total = 0
    for src_label in edge_bound.source_labels:
        src_bound = result.node_label_bounds.get(src_label)
        if src_bound is None or not src_bound.is_exact:
            return None
        total += src_bound.min_count
    if total == 0:
        return None
    return round(edge_bound.min_count / total, 2)


def pg_fallback_report_payload(
    summary: PGSummary,
    fallback: FallbackResult,
    *,
    endpoint: str | None = None,
    multi_label: MultiLabelProbeResult | None = None,
) -> dict[str, object]:
    """Return the concise PG fallback report as a JSON-serializable dict.

    Fallback mode runs when ``neptune.graph.pg_schema`` isn't available on
    the cluster (older engine) — we get bare node/edge counts but no
    property or source/target detail. The payload shape reflects that.
    """
    payload: dict[str, object] = {
        "endpoint": endpoint or "",
        "mode": "pg",
        "fallback": True,
        "totals": {"nodes": summary.num_nodes, "edges": summary.num_edges},
        "node_counts": dict(fallback.node_counts),
        "edge_counts": dict(fallback.edge_counts),
    }
    if fallback.failed_node_labels:
        payload["failed_node_labels"] = list(fallback.failed_node_labels)
    if fallback.failed_edge_labels:
        payload["failed_edge_labels"] = list(fallback.failed_edge_labels)
    ml = _multi_label_payload(multi_label)
    if ml is not None:
        payload["multi_label"] = ml
    return payload


def rdf_fallback_report_payload(
    fallback: RDFFallbackResult,
    *,
    endpoint: str | None = None,
    reason: str | None = None,
) -> dict[str, object]:
    """Return the concise RDF fallback report as a JSON-serializable dict.

    Fallback mode runs when the DFE statistics engine can't serve the
    RDF summary API (limit reached, etc.). We report whatever bounded
    aggregates the query engine can still compute.
    """
    payload: dict[str, object] = {
        "endpoint": endpoint or "",
        "mode": "rdf",
        "fallback": True,
    }
    if reason:
        payload["reason"] = reason
    totals: dict[str, object] = {}
    if fallback.total_triples is not None:
        totals["total_triples"] = fallback.total_triples
    if fallback.distinct_subjects is not None:
        totals["distinct_subjects"] = fallback.distinct_subjects
    if fallback.distinct_predicates is not None:
        totals["distinct_predicates"] = fallback.distinct_predicates
    if totals:
        payload["totals"] = totals
    if fallback.class_counts:
        payload["class_counts"] = dict(fallback.class_counts)
    if fallback.failed_queries:
        payload["failed_queries"] = list(fallback.failed_queries)
    return payload


def _multi_label_payload(
    probe: MultiLabelProbeResult | None,
) -> dict[str, object] | None:
    if probe is None:
        return None
    return {
        "pairs_checked": probe.pairs_checked,
        "overlaps": [
            {
                "labels": list(overlap.labels),
                "count": overlap.count,
            }
            for overlap in probe.overlaps
        ],
    }
