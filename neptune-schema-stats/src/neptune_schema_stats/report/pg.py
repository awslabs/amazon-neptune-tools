"""Property-graph (PG) report rendering — both concise and detailed views."""

from __future__ import annotations

from io import StringIO

from tabulate import tabulate

from neptune_schema_stats.client.statistics import StatisticsInfo
from neptune_schema_stats.correlator import (
    AmbiguousNodeGroup,
    LabelSetCount,
    LabelStats,
    PGCorrelationResult,
)
from neptune_schema_stats.models import PGSchema, PGSummary
from neptune_schema_stats.multi_label import MultiLabelProbeResult
from neptune_schema_stats.report._shared import (
    AMBIGUITY_MARKER,
    DASH,
    EXACT_MARKER,
    NUM_TABLEFMT,
)
from neptune_schema_stats.report._shared import (
    pct as _pct,
)
from neptune_schema_stats.scan import ScanResults


def render_pg_report(
    summary: PGSummary,
    schema: PGSchema,
    result: PGCorrelationResult,
    *,
    endpoint: str | None = None,
    multi_label: MultiLabelProbeResult | None = None,
    scan: ScanResults | None = None,
    statistics: StatisticsInfo | None = None,
    details: bool = False,
) -> str:
    """Render the PG statistics report.

    Two modes:

    - ``details=False`` (default): a concise header + nodes + edges view.
      Progress/context sections are omitted; users saw them on stderr as
      progress logs while the tool was running.
    - ``details=True``: the extended report with ambiguity ranges, edges by
      target label, structure breakdowns, consistency warnings, scan detail,
      and a legend.

    When available, ``statistics`` (DFE statistics snapshot from
    ``/pg/statistics``) is displayed in the header alongside the summary /
    schema timestamps — a stale statistics id is a good early indicator
    that the summary counts may lag the graph.
    """
    if details:
        return _render_pg_detailed(
            summary=summary,
            schema=schema,
            result=result,
            endpoint=endpoint,
            multi_label=multi_label,
            scan=scan,
            statistics=statistics,
        )
    return _render_pg_concise(
        summary=summary,
        schema=schema,
        result=result,
        endpoint=endpoint,
        multi_label=multi_label,
        scan=scan,
        statistics=statistics,
    )


def _render_pg_concise(
    *,
    summary: PGSummary,
    schema: PGSchema,
    result: PGCorrelationResult,
    endpoint: str | None,
    multi_label: MultiLabelProbeResult | None,
    scan: ScanResults | None,
    statistics: StatisticsInfo | None = None,
) -> str:
    """Concise report focused on nodes and edges only."""
    buf = StringIO()

    # Compact header.
    buf.write("Property Graph Statistics\n")
    buf.write("=" * 25 + "\n")
    if endpoint:
        buf.write(f"Endpoint:      {endpoint}\n")
    buf.write(f"Total nodes:   {summary.num_nodes:>15,}\n")
    buf.write(f"Total edges:   {summary.num_edges:>15,}\n")
    if statistics is not None and statistics.date is not None:
        buf.write(f"Stats updated: {statistics.date}\n")
    buf.write("\n")

    _write_concise_nodes(buf, result, scan_applied=scan is not None)
    buf.write("\n")
    _write_concise_edges(buf, result)

    # Only surface footnotes for anomalies or actionable issues.
    notes: list[str] = []
    if multi_label is not None and multi_label.any_multi_label and not result.label_partition:
        # No partition was computed (either scans were skipped or the probe
        # data didn't reach apply_scan). Single-label counts in the table
        # above include multi-labeled nodes and therefore over-count.
        notes.append(
            f"{AMBIGUITY_MARKER} Multi-labeled nodes detected in {len(multi_label.hits)} "
            "label pair(s). Per-label counts include multi-labeled nodes and will "
            "sum to more than the graph total. Re-run without --api-only to get an "
            "exclusive-count partition."
        )
    if scan is None and _has_node_ranges(result):
        notes.append(
            "Some counts are ranges (min \u2014 max). Scans were skipped "
            "(--api-only). Re-run without --api-only to resolve them to exact values."
        )
    if (
        scan is None
        and any(b.is_range for b in result.edge_label_bounds.values())
        and not any("ranges" in n for n in notes)
    ):
        notes.append(
            "Some edge counts are ranges (min \u2014 max). Scans were skipped "
            "(--api-only). Re-run without --api-only to resolve them to exact values."
        )
    if result.consistency_warnings:
        notes.append(
            f"{AMBIGUITY_MARKER} {len(result.consistency_warnings)} consistency "
            "warning(s). Re-run with --details for full information."
        )
    if notes:
        buf.write("\n")
        for n in notes:
            buf.write(f"  {n}\n")
    return buf.getvalue()


def _write_concise_nodes(
    buf: StringIO,
    result: PGCorrelationResult,
    *,
    scan_applied: bool,
) -> None:
    buf.write("Node labels\n")
    buf.write("-" * 11 + "\n")

    # Build a lookup of partition entries by label(set) so we can substitute
    # exclusive counts and split off multi-labelset rows into their own table.
    partition_singleton_counts: dict[str, int] = {}
    partition_multi: list[LabelSetCount] = []
    for entry in result.label_partition:
        if entry.is_multi:
            partition_multi.append(entry)
        else:
            partition_singleton_counts[entry.labels[0]] = entry.count

    bounds = sorted(
        result.node_label_bounds.values(),
        key=lambda b: (-b.max_count, b.label),
    )
    rows: list[list[object]] = []
    for b in bounds:
        # If this label has an exclusive partition entry, prefer that count
        # (the raw scan/bound would include multi-labeled nodes).
        if b.label in partition_singleton_counts:
            count = f"{partition_singleton_counts[b.label]:,}"
        else:
            count = _format_bound_count(b.min_count, b.max_count)
        stats = result.label_stats.get(b.label)
        # Use stats.node_count as the fill denominator so percentages reflect
        # the population we could actually attribute (rather than the max-side
        # of a range, which would understate rates for ambiguous labels).
        props = _format_property_fill_dynamic(stats, stats.node_count) if stats else DASH
        rows.append([b.label, count, props])

    buf.write(
        tabulate(
            rows,
            headers=["Label", "Count", "Properties"],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "right", "left"),
        )
    )
    buf.write("\n")

    # Multi-labelset rows go in a separate table so they don't visually
    # collide with any literal label that happens to contain a colon.
    if partition_multi:
        buf.write("\nMulti-label combinations\n")
        buf.write("-" * 24 + "\n")
        multi_rows = [
            [entry.display_name(), f"{entry.count:,}"]
            for entry in sorted(partition_multi, key=lambda e: (-e.count, e.labels))
        ]
        buf.write(
            tabulate(
                multi_rows,
                headers=["Labels", "Count"],
                tablefmt=NUM_TABLEFMT,
                colalign=("left", "right"),
            )
        )
        buf.write(
            "\n\n  Nodes carrying more than one label. The Node labels table above shows\n"
            "  exclusive counts (nodes carrying only that label among the detected\n"
            "  combinations), so single + multi rows partition the graph without\n"
            "  double-counting.\n"
        )

    _ = scan_applied  # reserved for future decoration


def _write_concise_edges(buf: StringIO, result: PGCorrelationResult) -> None:
    buf.write("Edge labels\n")
    buf.write("-" * 11 + "\n")

    bounds = sorted(
        result.edge_label_bounds.values(),
        key=lambda b: (-b.max_count, b.label),
    )
    rows: list[list[object]] = []
    for b in bounds:
        count = f"{b.min_count:,}" if b.is_exact else f"{b.min_count:,} \u2014 {b.max_count:,}"
        pair = _source_target_summary(result, b.label)
        props = ", ".join(b.property_signature) if b.property_signature else DASH
        mean = _mean_per_source(result, b.label) if b.is_exact else DASH
        rows.append([b.label, count, mean, pair, props])
    buf.write(
        tabulate(
            rows,
            headers=[
                "Edge label",
                "Count",
                "Mean/src",
                "Source \u2192 Target",
                "Properties",
            ],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "right", "right", "left", "left"),
        )
    )
    buf.write("\n")


def _source_target_summary(
    result: PGCorrelationResult,
    edge_label: str,
) -> str:
    """Return a compact ``src → tgt`` string for an edge label. Preserves
    the actual (source, target) pairings when the edge type appears in
    multiple distinct patterns in the schema's labelTriples.

    Rendering rules, in order:

    - Single pair ``[(A, B)]`` → ``A → B``.
    - Multiple pairs sharing one target ``[(A,X), (B,X)]`` → ``A, B → X``.
    - Multiple pairs sharing one source ``[(A,X), (A,Y)]`` → ``A → X, Y``.
    - Otherwise, distinct pairings shown explicitly:
      ``[(A,X), (B,Y)]`` → ``A → X; B → Y``.
    """
    b = result.edge_label_bounds.get(edge_label)
    if b is None:
        return DASH

    pairs = b.source_target_pairs
    if pairs:
        return _format_source_target_pairs(pairs)

    # Fallback for older correlations that didn't populate pairs: use the
    # flattened source/target sets from bounds and edges_by_target_label.
    sources = list(b.source_labels)
    targets = sorted(
        label
        for label, tgt in result.edges_by_target_label.items()
        if edge_label in tgt.contributing_edge_labels
    )
    src_str = ", ".join(sources) if sources else "?"
    tgt_str = ", ".join(targets) if targets else "?"
    return f"{src_str} \u2192 {tgt_str}"


def _format_source_target_pairs(pairs: tuple[tuple[str, str], ...]) -> str:
    """See :func:`_source_target_summary` for the rules."""
    if not pairs:
        return "?"
    unique_pairs = sorted(set(pairs))
    if len(unique_pairs) == 1:
        s, t = unique_pairs[0]
        return f"{s} \u2192 {t}"

    unique_sources = {s for s, _ in unique_pairs}
    unique_targets = {t for _, t in unique_pairs}
    if len(unique_targets) == 1:
        target = next(iter(unique_targets))
        srcs = ", ".join(sorted(unique_sources))
        return f"{srcs} \u2192 {target}"
    if len(unique_sources) == 1:
        source = next(iter(unique_sources))
        tgts = ", ".join(sorted(unique_targets))
        return f"{source} \u2192 {tgts}"
    # Truly cross-product patterns: render each pair explicitly, joined by ``;``.
    return "; ".join(f"{s} \u2192 {t}" for s, t in unique_pairs)


def _mean_per_source(result: PGCorrelationResult, edge_label: str) -> str:
    """Return the mean number of edges emitted per source node for the given
    edge label, formatted for display. Only meaningful when both the edge's
    count and its sources' counts are exact. Returns ``DASH`` if either input
    is a range, or if the source count is zero.
    """
    edge_bound = result.edge_label_bounds.get(edge_label)
    if edge_bound is None or not edge_bound.is_exact:
        return DASH
    total_source_count = 0
    for src_label in edge_bound.source_labels:
        src_bound = result.node_label_bounds.get(src_label)
        if src_bound is None or not src_bound.is_exact:
            return DASH
        total_source_count += src_bound.min_count
    if total_source_count == 0:
        return DASH
    mean = edge_bound.min_count / total_source_count
    if mean < 10:
        return f"{mean:.1f}"
    return f"{mean:,.0f}"


def _render_pg_detailed(
    *,
    summary: PGSummary,
    schema: PGSchema,
    result: PGCorrelationResult,
    endpoint: str | None,
    multi_label: MultiLabelProbeResult | None,
    scan: ScanResults | None,
    statistics: StatisticsInfo | None = None,
) -> str:
    """Extended report — everything the tool knows."""
    buf = StringIO()

    _write_header(buf, summary, schema, endpoint=endpoint, statistics=statistics)
    buf.write("\n")
    if multi_label is not None:
        _write_multi_label_section(buf, multi_label)
        buf.write("\n")
    if scan is not None:
        _write_scan_summary_section(buf, scan)
        buf.write("\n")
    if result.consistency_warnings:
        _write_consistency_warnings_section(buf, result)
        buf.write("\n")
    _write_node_labels_section(buf, result, scan_applied=scan is not None)
    buf.write("\n")
    if _has_node_ranges(result):
        _write_node_label_bounds_section(buf, result, summary)
        buf.write("\n")
    # Only show the ambiguous-groups section when the ambiguity is
    # unresolved. When a scan ran, per-label totals in the node-labels
    # table above are already exact — repeating the pre-scan ambiguity is
    # noise.
    if result.ambiguous_node_groups and scan is None:
        _write_ambiguous_groups_section(
            buf,
            result.ambiguous_node_groups,
            summary,
            scan_applied=False,
        )
        buf.write("\n")
    if result.unmapped_node_structures:
        _write_unmapped_section(buf, result)
        buf.write("\n")
    _write_edge_labels_section(buf, result, summary)
    buf.write("\n")
    _write_edges_by_target_section(buf, result, summary)

    return buf.getvalue()


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------


def _write_header(
    buf: StringIO,
    summary: PGSummary,
    schema: PGSchema,
    *,
    endpoint: str | None,
    statistics: StatisticsInfo | None = None,
) -> None:
    lines = ["Property Graph Statistics", "=" * 25]
    if endpoint:
        lines.append(f"Endpoint:            {endpoint}")
    lines.append(f"Total nodes:         {summary.num_nodes:>15,}")
    lines.append(f"Total edges:         {summary.num_edges:>15,}")
    lines.append(f"Node labels:         {summary.num_node_labels:>15,}")
    lines.append(f"Edge labels:         {summary.num_edge_labels:>15,}")
    lines.append(f"Summary computed:    {summary.last_statistics_computation_time}")
    lines.append(f"Schema computed:     {schema.status.last_computed_timestamp}")
    lines.append(f"Schema state:        {schema.state().value}")
    if statistics is not None:
        lines.append(f"Statistics id:       {statistics.statistics_id}")
        if statistics.date is not None:
            lines.append(f"Statistics updated:  {statistics.date}")
        if statistics.instance_count is not None:
            lines.append(f"Statistics coverage: {statistics.instance_count:>15,} instances")
    buf.write("\n".join(lines))
    buf.write("\n")


def _write_multi_label_section(buf: StringIO, probe: MultiLabelProbeResult) -> None:
    buf.write("Multi-label detection\n")
    buf.write("-" * 21 + "\n")
    if not probe.any_multi_label:
        buf.write(
            f"Checked {probe.pairs_checked} label pairs. "
            "No multi-labeled nodes found — per-label counts below are unique.\n"
        )
        return

    rows = [
        [pair.display(), f"{pair.node_count:,}"]
        for pair in sorted(probe.hits, key=lambda p: (-p.node_count, p.labels))
    ]
    buf.write(f"Checked {probe.pairs_checked} label pairs. Found ")
    buf.write(f"{len(probe.hits)} pair(s) with multi-labeled nodes:\n\n")
    buf.write(
        tabulate(
            rows,
            headers=["Label pair", "Nodes"],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "right"),
        )
    )
    buf.write("\n")


def _has_node_ranges(result: PGCorrelationResult) -> bool:
    return any(b.is_range for b in result.node_label_bounds.values())


def _format_bound_count(min_count: int, max_count: int) -> str:
    """Render a bound as a single count or a range depending on exactness."""
    if min_count == max_count:
        return f"{min_count:,}"
    return f"{min_count:,} \u2014 {max_count:,}"


def _write_scan_summary_section(buf: StringIO, scan: ScanResults) -> None:
    plan = scan.plan
    buf.write(f"Scan summary ({EXACT_MARKER} bounds collapsed to exact via targeted queries)\n")
    buf.write("-" * 62 + "\n")

    total = plan.total_queries

    buf.write(
        f"Queries issued: {total}"
        f"  (nodes: {len(plan.node_labels_to_query)}, "
        f"edges: {len(plan.edge_labels_to_query)})\n"
    )
    if scan.failed_node_labels or scan.failed_edge_labels:
        buf.write(
            f"{AMBIGUITY_MARKER} Failed queries: "
            f"{len(scan.failed_node_labels)} node, {len(scan.failed_edge_labels)} edge "
            "— original bounds preserved for failed items.\n"
        )

    # Node-level detail.
    if scan.node_scans:
        buf.write("\nNode counts resolved:\n")
        rows: list[list[object]] = [[s.label, f"{s.exact_count:,}"] for s in scan.node_scans]
        buf.write(
            tabulate(
                rows,
                headers=["Label", "Count"],
                tablefmt=NUM_TABLEFMT,
                colalign=("left", "right"),
            )
        )
        buf.write("\n")

    # Edge-level detail.
    if scan.edge_scans:
        buf.write("\nEdge counts resolved:\n")
        rows2: list[list[object]] = [[s.label, f"{s.exact_count:,}"] for s in scan.edge_scans]
        buf.write(
            tabulate(
                rows2,
                headers=["Edge label", "Count"],
                tablefmt=NUM_TABLEFMT,
                colalign=("left", "right"),
            )
        )
        buf.write("\n")


def _write_consistency_warnings_section(buf: StringIO, result: PGCorrelationResult) -> None:
    buf.write(f"{AMBIGUITY_MARKER} Consistency warnings\n")
    buf.write("-" * 22 + "\n")
    rows: list[list[object]] = []
    for w in result.consistency_warnings:
        rows.append(
            [
                w.kind.replace("_", " "),
                w.subject,
                f"{w.expected:,}",
                f"{w.actual:,}",
                w.detail,
            ]
        )
    buf.write(
        tabulate(
            rows,
            headers=["Kind", "Subject", "Expected", "Actual", "Detail"],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "left", "right", "right", "left"),
        )
    )
    buf.write("\n")


def _write_node_label_bounds_section(
    buf: StringIO,
    result: PGCorrelationResult,
    summary: PGSummary,
) -> None:
    buf.write("Node label bounds (accounts for ambiguity)\n")
    buf.write("-" * 42 + "\n")

    bounds_sorted = sorted(
        result.node_label_bounds.values(),
        key=lambda b: (-b.max_count, b.label),
    )
    rows: list[list[object]] = []
    for b in bounds_sorted:
        # Include every non-empty label so readers can see how a small "min"
        # can be dwarfed by ambiguous headroom.
        if b.max_count == 0:
            continue
        if b.is_exact:
            count_col = f"{b.min_count:,}"
            match_col = EXACT_MARKER
            pct_col = _pct(b.min_count, summary.num_nodes)
        else:
            count_col = f"{b.min_count:,} \u2014 {b.max_count:,}"
            match_col = f"{AMBIGUITY_MARKER} range"
            pct_col = f"\u2264 {_pct(b.max_count, summary.num_nodes)}"
        rows.append([b.label, count_col, pct_col, match_col])

    buf.write(
        tabulate(
            rows,
            headers=["Label", "Count (min \u2014 max)", "% of total", "Match"],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "right", "right", "left"),
        )
    )
    buf.write("\n")
    buf.write(
        "\nRange bounds account for ambiguous node groups:\n"
        "  Min: nodes attributed via exact structure matches (strict lower bound).\n"
        "  Max: min plus every ambiguous group where the label is a candidate.\n"
        "  Note: individual labels' max values are not additive — sharing an\n"
        "  ambiguous group means both labels' max includes the same headroom.\n"
    )


def _write_edges_by_target_section(
    buf: StringIO,
    result: PGCorrelationResult,
    summary: PGSummary,
) -> None:
    buf.write("Edges by target label (from PG schema source→edge→target)\n")
    buf.write("-" * 49 + "\n")

    by_target_sorted = sorted(
        result.edges_by_target_label.values(),
        key=lambda t: (-t.max_count, t.label),
    )
    rows: list[list[object]] = []
    for t in by_target_sorted:
        count_col = f"{t.min_count:,}" if t.is_exact else f"{t.min_count:,} \u2014 {t.max_count:,}"
        edge_types = ", ".join(t.contributing_edge_labels) if t.contributing_edge_labels else DASH
        pct_col = "0.0%" if t.max_count == 0 else _pct(t.max_count, summary.num_edges)
        rows.append([t.label, count_col, pct_col, edge_types])

    buf.write(
        tabulate(
            rows,
            headers=["Target label", "Incoming edges (min \u2014 max)", "% of total", "Edge types"],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "right", "right", "left"),
        )
    )
    buf.write("\n")


def _write_node_labels_section(
    buf: StringIO,
    result: PGCorrelationResult,
    *,
    scan_applied: bool = False,
) -> None:
    if scan_applied:
        buf.write("Node labels (resolved via scan)\n")
        buf.write("-" * 31 + "\n")
    else:
        buf.write("Node labels (exact matches)\n")
        buf.write("-" * 27 + "\n")

    rows = _node_label_rows(result, scan_applied=scan_applied)
    if rows:
        buf.write(
            tabulate(
                rows,
                headers=["Label", "Count", "Structures", "Properties", "Outgoing edges"],
                tablefmt=NUM_TABLEFMT,
                colalign=("left", "right", "right", "left", "left"),
            )
        )
        buf.write("\n")

    # Coverage summary
    exact = result.total_exact_nodes
    ambiguous = result.total_ambiguous_nodes
    unmapped = result.total_unmapped_nodes
    total = exact + ambiguous + unmapped
    buf.write("\n")
    if scan_applied:
        # When multi-label overlaps have been resolved into a partition, use
        # the partition-adjusted sum (exclusive singletons + multi-labelset
        # entries) so the total isn't inflated by multi-labeled nodes counted
        # under each of their labels.
        partition_singletons = {e.labels[0] for e in result.label_partition if not e.is_multi}
        if result.label_partition:
            resolved = sum(e.count for e in result.label_partition)
            resolved += sum(
                b.min_count
                for b in result.node_label_bounds.values()
                if b.is_exact and b.label not in partition_singletons
            )
        else:
            resolved = sum(b.min_count for b in result.node_label_bounds.values() if b.is_exact)
        buf.write(f"Resolved (scan): {resolved:>15,}  ({_pct(resolved, total)})\n")
        if unmapped:
            buf.write(f"Unmapped:        {unmapped:>15,}  ({_pct(unmapped, total)})\n")
    else:
        buf.write(f"Mapped exactly:  {exact:>15,}  ({_pct(exact, total)})\n")
        buf.write(f"Ambiguous:       {ambiguous:>15,}  ({_pct(ambiguous, total)})\n")
        if unmapped:
            buf.write(f"Unmapped:        {unmapped:>15,}  ({_pct(unmapped, total)})\n")


def _node_label_rows(
    result: PGCorrelationResult,
    *,
    scan_applied: bool = False,
) -> list[list[object]]:
    rows: list[list[object]] = []

    # Partition lookup: exclusive counts for singletons, plus the list of
    # multi-labelset combinations to append.
    partition_singleton_counts: dict[str, int] = {}
    partition_multi: list[LabelSetCount] = []
    for entry in result.label_partition:
        if entry.is_multi:
            partition_multi.append(entry)
        else:
            partition_singleton_counts[entry.labels[0]] = entry.count

    # Post-scan: read counts from node_label_bounds (which are exact after scan).
    # Pre-scan: read from label_stats (correlation exact-only attribution).
    def _count_for(label: str) -> int:
        if label in partition_singleton_counts:
            return partition_singleton_counts[label]
        if scan_applied:
            b = result.node_label_bounds.get(label)
            if b is not None and b.is_exact:
                return b.min_count
        return result.label_stats[label].node_count

    labels_sorted = sorted(
        result.label_stats.values(),
        key=lambda s: (-_count_for(s.label), s.label),
    )
    for stats in labels_sorted:
        count = _count_for(stats.label)
        if count == 0 and stats.contributing_structures == 0:
            continue
        rows.append(
            [
                stats.label,
                f"{count:,}",
                stats.contributing_structures,
                _format_property_fill_dynamic(stats, count),
                _format_outgoing_edges(stats, result),
            ]
        )
    # Append 0-count labels after non-zero rows.
    for stats in labels_sorted:
        count = _count_for(stats.label)
        if count == 0 and stats.contributing_structures == 0:
            rows.append(
                [
                    stats.label,
                    "0",
                    0,
                    _format_property_fill_dynamic(stats, 0),
                    _format_outgoing_edges(stats, result),
                ]
            )

    # Append multi-labelset partition rows below the singleton rows.
    # (Detailed report keeps them in the same table since it has more columns
    # and the "Structures" column of "—" is a clear differentiator.)
    for entry in sorted(partition_multi, key=lambda e: (-e.count, e.labels)):
        rows.append(
            [
                f"[multi] {entry.display_name()}",
                f"{entry.count:,}",
                "—",
                "(nodes carrying all labels)",
                "—",
            ]
        )
    return rows


def _format_property_fill_dynamic(stats: LabelStats, current_count: int) -> str:
    """Render property fill rates using the *current* count as the denominator.

    Pre-scan, current_count equals stats.node_count and behavior is identical
    to :func:`_format_property_fill`. Post-scan the total-for-this-label
    may be larger (some ambiguous nodes are now attributed to this label);
    unpopulated fills stay at their original absolute counts, so the fill
    percentage naturally drops. This surfaces "this label has X% property Y
    coverage" honestly — for example, in the identity-graph dataset, only
    12.7% of websites carry a ``title`` property after ambiguity is resolved.

    Display rules (chosen so the marker unambiguously reflects the ratio):

    - ``fills == count`` — bare property name (exact 100%).
    - ``fills == 0`` — ``prop:0%``.
    - ``0 < pct < 1`` — ``prop:<1%`` (avoids showing "0%" for tiny non-zero).
    - anything else — ``prop:XX%`` with **floor** rounding, so a fill of
      99.6% shows as ``prop:99%`` rather than ``prop:100%``. This guarantees
      that the ``%`` marker never lies about whether a property is fully
      populated.
    """
    if not stats.property_fill_counts:
        return DASH
    if current_count == 0:
        return ", ".join(stats.property_fill_counts.keys())
    parts: list[str] = []
    for prop, fills in stats.property_fill_counts.items():
        parts.append(_format_fill_marker(prop, fills, current_count))
    return ", ".join(parts)


def _format_property_fill(stats: LabelStats) -> str:
    """Render property fill rates compactly: ``prop`` if exactly 100%
    populated, otherwise ``prop:XX%`` using the same display rules as
    :func:`_format_property_fill_dynamic`."""
    if not stats.property_fill_counts:
        return DASH
    if stats.node_count == 0:
        return ", ".join(stats.property_fill_counts.keys())
    parts: list[str] = []
    for prop, fills in stats.property_fill_counts.items():
        parts.append(_format_fill_marker(prop, fills, stats.node_count))
    return ", ".join(parts)


def _format_fill_marker(prop: str, fills: int, count: int) -> str:
    """Format a single ``prop`` / ``prop:XX%`` marker for the properties column."""
    if fills == count:
        return prop  # exact 100%
    if fills == 0:
        return f"{prop}:0%"
    pct = 100.0 * fills / count
    if pct < 1:
        return f"{prop}:<1%"
    # Floor so a 99.6% fill never renders as 100%.
    return f"{prop}:{int(pct)}%"


def _format_outgoing_edges(stats: LabelStats, result: PGCorrelationResult) -> str:
    sig = result.label_index.get(stats.label)
    if sig is None or not sig.valid_outgoing:
        return DASH
    return ", ".join(sorted(sig.valid_outgoing))


def _write_ambiguous_groups_section(
    buf: StringIO,
    groups: tuple[AmbiguousNodeGroup, ...],
    summary: PGSummary,
    *,
    scan_applied: bool = False,
) -> None:
    """Render the ambiguous-groups section. Only meaningful when the
    ambiguity is unresolved (no scan ran); callers should suppress the
    section entirely when scans have collapsed each candidate to an exact
    per-label total."""
    _ = scan_applied  # legacy — retained for callers that still pass it
    buf.write(f"{AMBIGUITY_MARKER} Ambiguous node groups (re-run without --api-only to resolve)\n")
    buf.write("-" * 55 + "\n")

    rows: list[list[object]] = []
    for group in groups:
        pct = _pct(group.total_count, summary.num_nodes)
        candidate_str = " | ".join(group.candidate_labels)
        rows.append(
            [
                candidate_str,
                f"{group.total_count:,}",
                pct,
                len(group.structures),
            ]
        )
    buf.write(
        tabulate(
            rows,
            headers=["Candidate labels", "Nodes", "% of total", "Structures"],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "right", "right", "right"),
        )
    )
    buf.write("\n")


def _write_unmapped_section(buf: StringIO, result: PGCorrelationResult) -> None:
    buf.write(f"{AMBIGUITY_MARKER} Unmapped node structures (no matching label)\n")
    buf.write("-" * 45 + "\n")
    rows = [
        [
            f"{s.count:,}",
            ", ".join(s.node_properties) or DASH,
            ", ".join(s.distinct_outgoing_edge_labels) or DASH,
        ]
        for s in result.unmapped_node_structures
    ]
    buf.write(
        tabulate(
            rows,
            headers=["Count", "Properties", "Outgoing edges"],
            tablefmt=NUM_TABLEFMT,
            colalign=("right", "left", "left"),
        )
    )
    buf.write("\n")


def _write_edge_labels_section(
    buf: StringIO,
    result: PGCorrelationResult,
    summary: PGSummary,
) -> None:
    buf.write("Edge labels\n")
    buf.write("-" * 11 + "\n")

    bounds_sorted = sorted(
        result.edge_label_bounds.values(),
        key=lambda b: (-b.max_count, b.label),
    )
    rows: list[list[object]] = []
    for b in bounds_sorted:
        if b.is_exact:
            count_col = f"{b.min_count:,}"
            match_col = EXACT_MARKER
            pct_col = _pct(b.min_count, summary.num_edges)
        else:
            count_col = f"{b.min_count:,} \u2014 {b.max_count:,}"
            match_col = f"{AMBIGUITY_MARKER} range"
            # Percentage of total is ambiguous for a range — show the max side.
            pct_col = f"\u2264 {_pct(b.max_count, summary.num_edges)}"
        mean_col = _mean_per_source(result, b.label) if b.is_exact else DASH
        source_col = ", ".join(b.source_labels) if b.source_labels else DASH
        signature_col = ", ".join(b.property_signature) if b.property_signature else DASH
        rows.append([b.label, count_col, mean_col, pct_col, source_col, signature_col, match_col])

    if rows:
        buf.write(
            tabulate(
                rows,
                headers=[
                    "Edge label",
                    "Count (min \u2014 max)",
                    "Mean/src",
                    "% of total",
                    "Source label(s)",
                    "Properties",
                    "Match",
                ],
                tablefmt=NUM_TABLEFMT,
                colalign=(
                    "left",
                    "right",
                    "right",
                    "right",
                    "left",
                    "left",
                    "left",
                ),
            )
        )
        buf.write("\n")
    else:
        buf.write(f"{DASH}\n")

    # Explanatory footnote when any bound is a range.
    if any(b.is_range for b in bounds_sorted):
        buf.write("\n")
        buf.write(
            "Range bounds are derived from node characteristic sets:\n"
            "  Min: each source-node in structures declaring this outgoing edge\n"
            "       type emits at least one such edge (strict lower bound).\n"
            "  Max: total edges with the shared property signature minus the\n"
            "       sum of the other candidate labels' minimums.\n"
            "  Re-run without --api-only for exact per-label edge counts.\n"
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
