"""RDF flow — default report, class-count probe, and SPARQL fallback."""

from __future__ import annotations

import argparse
import logging
import sys

from neptune_schema_stats.cli._constants import EXIT_OK, EXIT_STATS_UNAVAILABLE
from neptune_schema_stats.cli.entry import _emit_json, _to_jsonable
from neptune_schema_stats.cli.hints import (
    _check_statistics_limit,
    _fetch_statistics_best_effort,
    _print_statistics_limit_reached_hint,
)
from neptune_schema_stats.client import NeptuneClient, NeptuneClientError, fetch_rdf_summary
from neptune_schema_stats.client.sparql import sparql_class_counts
from neptune_schema_stats.client.statistics import RDF_STATISTICS_PATH
from neptune_schema_stats.correlator.rdf import correlate_rdf
from neptune_schema_stats.fallback.rdf import fetch_rdf_fallback
from neptune_schema_stats.models import GraphMode
from neptune_schema_stats.report import (
    rdf_fallback_report_payload,
    rdf_report_payload,
    render_rdf_fallback_report,
    render_rdf_report,
)

log = logging.getLogger(__name__)


def _run_rdf_default(client: NeptuneClient, args: argparse.Namespace) -> int:
    """RDF default flow: fetch summary, run class-count probe (default-on),
    correlate, render report (or emit JSON on --dump/--json).

    When DFE statistics are unavailable (limit reached, inactive, or the
    summary API returns StatisticsNotAvailableException) the flow branches
    to a SPARQL-only fallback that reports whatever bounded aggregates the
    query engine can still compute. ``--api-only`` opts out — no fallback
    queries run and the tool exits with an actionable hint instead.
    """
    # Check DFE statistics status before fetching the summary. If the DFE
    # hit its limit (or stats are otherwise inactive), the summary API
    # will fail with a less helpful error. Fall back to SPARQL aggregates
    # unless the user requested --api-only (no query I/O at all).
    rdf_stats = _fetch_statistics_best_effort(client, RDF_STATISTICS_PATH)
    if rdf_stats is not None and not rdf_stats.is_usable:
        if args.api_only:
            # Propagate the limit hint and exit — no SPARQL fallback.
            return _check_statistics_limit(rdf_stats, RDF_STATISTICS_PATH) or EXIT_STATS_UNAVAILABLE
        # Print the same hint to stderr so the operator sees why we
        # dropped into fallback, then proceed to the SPARQL-only path.
        _print_statistics_limit_reached_hint(rdf_stats, RDF_STATISTICS_PATH)
        log.warning("Falling back to SPARQL aggregate queries.")
        return _run_rdf_fallback(client, args, reason=rdf_stats.note)

    rdf_summary = fetch_rdf_summary(client)

    if args.dump:
        return _emit_json(
            {
                "endpoint": client.base_url,
                "mode": GraphMode.RDF.value,
                "rdf_summary": _to_jsonable(rdf_summary),
            }
        )

    class_counts = _run_class_count_probe(client, args)
    result = correlate_rdf(rdf_summary, class_counts=class_counts)

    if args.json:
        payload = rdf_report_payload(
            rdf_summary,
            result,
            endpoint=client.base_url,
            statistics=rdf_stats,
        )
        return _emit_json(payload)

    sys.stdout.write(
        render_rdf_report(rdf_summary, result, endpoint=client.base_url, details=args.details)
    )
    return EXIT_OK


def _run_class_count_probe(
    client: NeptuneClient,
    args: argparse.Namespace,
) -> dict[str, int] | None:
    """Run the SPARQL class-count probe.

    Failures log a warning and return None; the correlator still runs without
    class counts (falls back to the API-only view). This mirrors the graceful
    degradation of the PG multi-label probe.
    """
    _ = args  # kept for future opt-out hooks; the probe is unconditional today
    try:
        return sparql_class_counts(client)
    except NeptuneClientError as exc:
        log.warning("SPARQL class-count probe failed: %s", exc)
        sys.stderr.write(
            f"warning: class-count probe failed ({exc}); rendering RDF report "
            "without class distribution.\n"
        )
        return None


def _run_rdf_fallback(
    client: NeptuneClient,
    args: argparse.Namespace,
    *,
    reason: str | None = None,
) -> int:
    """SPARQL-only RDF report path. Called when the DFE statistics engine
    can't serve the summary API and ``--api-only`` was not passed.

    Runs bounded aggregate queries directly against the SPARQL endpoint —
    total triples, distinct subjects/predicates, class distribution — and
    renders the reduced report.
    """
    log.info(
        "Running SPARQL fallback queries (total triples, distinct subjects/predicates, classes)"
    )
    fallback = fetch_rdf_fallback(client)

    if args.json:
        payload = rdf_fallback_report_payload(fallback, endpoint=client.base_url, reason=reason)
        return _emit_json(payload)

    sys.stdout.write(
        render_rdf_fallback_report(
            fallback,
            endpoint=client.base_url,
            reason=reason,
        )
    )
    return EXIT_OK
