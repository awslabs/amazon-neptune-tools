"""Fallback-mode report rendering (PG and RDF).

Both fallback flows produce a much shorter report — the summary API's
detailed characteristic sets aren't available. See
:mod:`neptune_schema_stats.fallback` for the query flows that populate the
result objects rendered here.
"""

from __future__ import annotations

from io import StringIO

from tabulate import tabulate

from neptune_schema_stats.fallback.pg import FallbackResult
from neptune_schema_stats.fallback.rdf import RDFFallbackResult
from neptune_schema_stats.models import PGSummary
from neptune_schema_stats.multi_label import MultiLabelProbeResult
from neptune_schema_stats.report._shared import (
    AMBIGUITY_MARKER,
    NUM_TABLEFMT,
)
from neptune_schema_stats.report._shared import (
    local_name as _local_name,
)


def render_pg_fallback_report(
    summary: PGSummary,
    fallback: FallbackResult,
    *,
    endpoint: str | None = None,
    multi_label: MultiLabelProbeResult | None = None,
) -> str:
    """Render a simplified report when ``neptune.graph.pg_schema`` is
    unavailable (Neptune < 1.4.8.0).

    Shows just node and edge label counts. Per-label properties, source →
    target labels, and characteristic-set correlation all require pg_schema
    and are omitted.
    """
    buf = StringIO()

    buf.write("Property Graph Statistics\n")
    buf.write("=" * 25 + "\n")
    if endpoint:
        buf.write(f"Endpoint:      {endpoint}\n")
    buf.write(f"Total nodes:   {summary.num_nodes:>15,}\n")
    buf.write(f"Total edges:   {summary.num_edges:>15,}\n")
    buf.write("\n")

    # Node labels
    buf.write("Node labels\n")
    buf.write("-" * 11 + "\n")
    node_rows = sorted(fallback.node_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    buf.write(
        tabulate(
            [[label, f"{count:,}"] for label, count in node_rows],
            headers=["Label", "Count"],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "right"),
        )
    )
    buf.write("\n")

    if fallback.failed_node_labels:
        buf.write(
            f"\n  {AMBIGUITY_MARKER} Failed to fetch counts for {len(fallback.failed_node_labels)} "
            f"node label(s): {', '.join(fallback.failed_node_labels)}\n"
        )

    buf.write("\n")

    # Edge labels
    buf.write("Edge labels\n")
    buf.write("-" * 11 + "\n")
    edge_rows = sorted(fallback.edge_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    buf.write(
        tabulate(
            [[label, f"{count:,}"] for label, count in edge_rows],
            headers=["Edge label", "Count"],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "right"),
        )
    )
    buf.write("\n")

    if fallback.failed_edge_labels:
        buf.write(
            f"\n  {AMBIGUITY_MARKER} Failed to fetch counts for {len(fallback.failed_edge_labels)} "
            f"edge label(s): {', '.join(fallback.failed_edge_labels)}\n"
        )

    # Multi-label footnote (fallback mode has no partition view yet, so
    # per-label counts really do include multi-labeled nodes and inflate).
    if multi_label is not None and multi_label.any_multi_label:
        buf.write(
            f"\n  {AMBIGUITY_MARKER} Multi-labeled nodes detected in {len(multi_label.hits)} "
            "label pair(s). Per-label counts include multi-labeled nodes and will\n"
            "    sum to more than the graph total.\n"
        )

    buf.write(
        "\n  \u26a0 Fallback mode (pg_schema unavailable): per-label properties and\n"
        "    edge source/target are omitted. Upgrade to Neptune 1.4.8.0+ for full report.\n"
    )

    return buf.getvalue()


def render_rdf_fallback_report(
    fallback: RDFFallbackResult,
    *,
    endpoint: str | None = None,
    reason: str | None = None,
) -> str:
    """Compact RDF report rendered from SPARQL aggregate queries alone.

    Used when the DFE statistics engine can't serve the RDF Graph Summary
    API (typically because the graph exceeded the DFE's characteristic-set
    limit). Reports only the metrics we could compute via bounded SPARQL
    queries — total triples, distinct subject/predicate counts, class
    distribution.

    :param reason: optional one-line description of *why* we're in fallback
        mode (e.g. Neptune's ``"Limit reached: Statistics are not
        available"`` note). Displayed in the header for context.
    """
    buf = StringIO()

    buf.write("RDF Graph Statistics (fallback mode)\n")
    buf.write("=" * 36 + "\n")
    if endpoint:
        buf.write(f"Endpoint:            {endpoint}\n")
    if reason:
        buf.write(f"Fallback reason:     {reason}\n")
    if fallback.total_triples is not None:
        buf.write(f"Total triples:       {fallback.total_triples:>15,}\n")
    else:
        buf.write("Total triples:       (query failed)\n")
    if fallback.distinct_subjects is not None:
        buf.write(f"Distinct subjects:   {fallback.distinct_subjects:>15,}\n")
    else:
        buf.write("Distinct subjects:   (query failed)\n")
    if fallback.distinct_predicates is not None:
        buf.write(f"Distinct predicates: {fallback.distinct_predicates:>15,}\n")
    else:
        buf.write("Distinct predicates: (query failed)\n")
    if fallback.class_counts:
        buf.write(f"Declared classes:    {len(fallback.class_counts):>15,}\n")

    # Class distribution table
    if fallback.class_counts:
        buf.write("\nClass distribution\n")
        buf.write("-" * 18 + "\n")
        rows = [
            [_local_name(uri), f"{count:,}", uri]
            for uri, count in sorted(
                fallback.class_counts.items(),
                key=lambda kv: (-kv[1], kv[0]),
            )
        ]
        buf.write(
            tabulate(
                rows,
                headers=["Class", "Subjects", "URI"],
                tablefmt=NUM_TABLEFMT,
                colalign=("left", "right", "left"),
            )
        )
        buf.write("\n")

    if fallback.failed_queries:
        buf.write(
            f"\n  {AMBIGUITY_MARKER} {len(fallback.failed_queries)} fallback quer(y|ies) failed:\n"
        )
        for msg in fallback.failed_queries:
            buf.write(f"    - {msg}\n")

    buf.write(
        "\n  \u26a0 Fallback mode: DFE statistics are unavailable, so\n"
        "    per-predicate occurrences and subject characteristic-set analysis\n"
        "    can't be reported. Resolve the DFE statistics limit (see hint\n"
        "    printed to stderr) for the full report.\n"
    )
    return buf.getvalue()
