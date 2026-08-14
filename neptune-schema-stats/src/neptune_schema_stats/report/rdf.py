"""RDF report rendering — both concise and detailed views."""

from __future__ import annotations

from io import StringIO

from tabulate import tabulate

from neptune_schema_stats.correlator.rdf import (
    RDFCorrelationResult,
    SubjectStructureAnalysis,
)
from neptune_schema_stats.models import RDFSummary
from neptune_schema_stats.report._shared import (
    AMBIGUITY_MARKER,
    DASH,
    NUM_TABLEFMT,
)
from neptune_schema_stats.report._shared import (
    local_name as _local_name,
)
from neptune_schema_stats.report._shared import (
    pct as _pct,
)

_MAX_PREDICATES_TO_RENDER = 30
_MAX_STRUCTURES_TO_RENDER = 30


def render_rdf_report(
    summary: RDFSummary,
    result: RDFCorrelationResult,
    *,
    endpoint: str | None = None,
    details: bool = False,
) -> str:
    """Render the RDF statistics report.

    Concise by default; ``details=True`` shows the full report including
    subject characteristic sets.
    """
    if details:
        return _render_rdf_detailed(
            summary=summary,
            result=result,
            endpoint=endpoint,
        )
    return _render_rdf_concise(
        summary=summary,
        result=result,
        endpoint=endpoint,
    )


def _render_rdf_concise(
    *,
    summary: RDFSummary,
    result: RDFCorrelationResult,
    endpoint: str | None,
) -> str:
    """Concise RDF report: header + typing + optional class distribution + predicates."""
    buf = StringIO()

    buf.write("RDF Graph Statistics\n")
    buf.write("=" * 20 + "\n")
    if endpoint:
        buf.write(f"Endpoint:            {endpoint}\n")
    buf.write(f"Distinct subjects:   {summary.num_distinct_subjects:>15,}\n")
    buf.write(f"Distinct predicates: {summary.num_distinct_predicates:>15,}\n")
    buf.write(f"Quads (triples):     {summary.num_quads:>15,}\n")
    buf.write(f"Declared classes:    {summary.num_classes:>15,}\n")
    buf.write("\n")

    _write_rdf_typing_section(buf, summary, result)
    buf.write("\n")
    if result.class_counts:
        _write_rdf_class_distribution_section(buf, summary, result)
        buf.write("\n")
    _write_rdf_predicate_section(buf, summary, result)
    return buf.getvalue()


def _render_rdf_detailed(
    *,
    summary: RDFSummary,
    result: RDFCorrelationResult,
    endpoint: str | None,
) -> str:
    """Full RDF report including subject characteristic sets."""
    buf = StringIO()

    _write_rdf_header(buf, summary, endpoint=endpoint)
    buf.write("\n")
    _write_rdf_typing_section(buf, summary, result)
    buf.write("\n")
    if result.class_counts:
        _write_rdf_class_distribution_section(buf, summary, result)
        buf.write("\n")
    _write_rdf_predicate_section(buf, summary, result)
    buf.write("\n")
    _write_rdf_structures_section(buf, summary, result)
    buf.write("\n")
    _write_rdf_legend(buf)

    return buf.getvalue()


def _write_rdf_header(buf: StringIO, summary: RDFSummary, *, endpoint: str | None) -> None:
    lines = ["RDF Graph Statistics", "=" * 20]
    if endpoint:
        lines.append(f"Endpoint:            {endpoint}")
    lines.append(f"Distinct subjects:   {summary.num_distinct_subjects:>15,}")
    lines.append(f"Distinct predicates: {summary.num_distinct_predicates:>15,}")
    lines.append(f"Quads (triples):     {summary.num_quads:>15,}")
    lines.append(f"Declared classes:    {summary.num_classes:>15,}")
    lines.append(f"Summary computed:    {summary.last_statistics_computation_time}")
    buf.write("\n".join(lines))
    buf.write("\n")


def _write_rdf_typing_section(
    buf: StringIO,
    summary: RDFSummary,
    result: RDFCorrelationResult,
) -> None:
    buf.write("Subject typing\n")
    buf.write("-" * 14 + "\n")

    total = summary.num_distinct_subjects
    typed = result.num_typed_subjects
    untyped = result.num_untyped_subjects

    rows = [
        ["Typed (has rdf:type)", f"{typed:,}", _pct(typed, total)],
        ["Untyped", f"{untyped:,}", _pct(untyped, total)],
    ]

    buf.write(
        tabulate(
            rows,
            headers=["Category", "Count", "% of total"],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "right", "right"),
        )
    )
    buf.write("\n")


def _write_rdf_class_distribution_section(
    buf: StringIO,
    summary: RDFSummary,
    result: RDFCorrelationResult,
) -> None:
    buf.write("Class distribution (from SPARQL class-count probe)\n")
    buf.write("-" * 50 + "\n")

    total_typed = result.num_typed_subjects
    rows: list[list[object]] = []
    for cc in result.class_counts:
        local = _local_name(cc.class_uri)
        rows.append(
            [
                local,
                f"{cc.subject_count:,}",
                _pct(cc.subject_count, total_typed) if total_typed else "0.0%",
                cc.class_uri,
            ]
        )

    if rows:
        buf.write(
            tabulate(
                rows,
                headers=["Class", "Subjects", "% of typed", "URI"],
                tablefmt=NUM_TABLEFMT,
                colalign=("left", "right", "right", "left"),
            )
        )
        buf.write("\n")
    else:
        buf.write(f"{DASH}\n")

    # Consistency note: if the sum of per-class counts differs from typed subjects,
    # some subjects have multiple rdf:type assignments.
    sum_class = result.num_typed_subjects_by_class
    if sum_class != total_typed and total_typed > 0:
        buf.write(
            "\n"
            f"{AMBIGUITY_MARKER} Sum of per-class subject counts ({sum_class:,}) "
            f"differs from typed subject count ({total_typed:,}).\n"
            "  Difference indicates subjects with multiple rdf:type assignments.\n"
        )

    _ = summary  # kept for future header/date use.


def _write_rdf_predicate_section(
    buf: StringIO,
    summary: RDFSummary,
    result: RDFCorrelationResult,
) -> None:
    buf.write("Predicates\n")
    buf.write("-" * 10 + "\n")

    total_quads = summary.num_quads
    predicates = result.predicate_stats
    top = predicates[:_MAX_PREDICATES_TO_RENDER]

    rows: list[list[object]] = []
    for ps in top:
        rows.append(
            [
                ps.local_name,
                f"{ps.occurrence_count:,}",
                _pct(ps.occurrence_count, total_quads) if total_quads else "0.0%",
                ps.uri,
            ]
        )

    buf.write(
        tabulate(
            rows,
            headers=["Local name", "Occurrences", "% of quads", "URI"],
            tablefmt=NUM_TABLEFMT,
            colalign=("left", "right", "right", "left"),
        )
    )
    buf.write("\n")

    if len(predicates) > _MAX_PREDICATES_TO_RENDER:
        buf.write(
            f"\n({len(predicates) - _MAX_PREDICATES_TO_RENDER} additional "
            "predicates omitted; use --json for the full list.)\n"
        )


def _write_rdf_structures_section(
    buf: StringIO,
    summary: RDFSummary,
    result: RDFCorrelationResult,
) -> None:
    buf.write("Subject characteristic sets\n")
    buf.write("-" * 27 + "\n")

    total = summary.num_distinct_subjects
    structures = sorted(
        result.subject_structures,
        key=lambda s: (-s.count, s.predicate_count),
    )
    top = structures[:_MAX_STRUCTURES_TO_RENDER]

    rows: list[list[object]] = []
    for s in top:
        rows.append(
            [
                f"{s.count:,}",
                _pct(s.count, total),
                s.predicate_count,
                _describe_structure(s),
                _describe_structure_predicates(s),
            ]
        )

    buf.write(
        tabulate(
            rows,
            headers=["Subjects", "% of total", "Preds", "Classification", "Predicates"],
            tablefmt=NUM_TABLEFMT,
            colalign=("right", "right", "right", "left", "left"),
        )
    )
    buf.write("\n")

    remaining = len(structures) - _MAX_STRUCTURES_TO_RENDER
    if remaining > 0:
        buf.write(f"\n({remaining} additional structures omitted; use --json for the full list.)\n")


def _write_rdf_legend(buf: StringIO) -> None:
    buf.write("Legend\n")
    buf.write("------\n")
    buf.write(
        "  All counts derived from the RDF Graph Summary API (and optionally a\n"
        "  single SPARQL class-count probe) — no full-graph scan performed.\n"
    )


# ---------------------------------------------------------------------------
# RDF report helpers
# ---------------------------------------------------------------------------


def _describe_structure(s: SubjectStructureAnalysis) -> str:
    if s.is_typed:
        return "typed"
    return "untyped"


def _describe_structure_predicates(s: SubjectStructureAnalysis) -> str:
    """Render a compact predicate list, prioritizing local names."""
    if not s.structure.predicates:
        return DASH
    parts = [_local_name(p) for p in s.structure.predicates]
    joined = ", ".join(parts)
    if len(joined) > 80:
        joined = joined[:77] + "..."
    return joined
