"""Property-graph flow — default report, fallback for older engines,
scan queries, and multi-label probe.
"""

from __future__ import annotations

import argparse
import logging
import sys

from neptune_schema_stats.cli._constants import EXIT_ERROR, EXIT_OK
from neptune_schema_stats.cli.entry import _emit_json, _to_jsonable
from neptune_schema_stats.cli.hints import (
    _check_statistics_limit,
    _fetch_statistics_best_effort,
    _print_schema_not_usable_hint,
)
from neptune_schema_stats.client import (
    NeptuneClient,
    NeptuneClientError,
    NeptuneHTTPError,
    fetch_pg_schema,
    fetch_pg_summary,
)
from neptune_schema_stats.client.statistics import PG_STATISTICS_PATH
from neptune_schema_stats.correlator import PGCorrelationResult, correlate_pg
from neptune_schema_stats.fallback.pg import fetch_label_counts, is_pg_schema_unavailable_error
from neptune_schema_stats.models import GraphMode, PGSchema, PGSummary
from neptune_schema_stats.multi_label import MultiLabelProbeResult, probe_multi_label_pairs
from neptune_schema_stats.report import (
    pg_fallback_report_payload,
    pg_report_payload,
    render_pg_fallback_report,
    render_pg_report,
)
from neptune_schema_stats.scan import ScanResults, apply_scan, execute_scan, plan_scan

log = logging.getLogger(__name__)


def _run_pg_default(client: NeptuneClient, args: argparse.Namespace) -> int:
    log.info("Fetching PG summary")
    summary = fetch_pg_summary(client)
    log.info(
        "Summary: %s nodes, %s edges, %d node labels, %d edge labels",
        f"{summary.num_nodes:,}",
        f"{summary.num_edges:,}",
        summary.num_node_labels,
        summary.num_edge_labels,
    )
    statistics = _fetch_statistics_best_effort(client)
    limit_exit = _check_statistics_limit(statistics, PG_STATISTICS_PATH)
    if limit_exit is not None:
        return limit_exit
    log.info("Fetching PG schema")
    try:
        schema = fetch_pg_schema(client)
    except NeptuneHTTPError as exc:
        if is_pg_schema_unavailable_error(exc):
            if args.api_only:
                # --api-only forbids fallback queries; propagate the error.
                raise
            return _run_pg_fallback(client, summary, args)
        raise

    if args.dump:
        _dump_pg_raw(client, summary, schema)
        return EXIT_OK

    if not schema.is_usable():
        _print_schema_not_usable_hint(schema, client.base_url)
        return EXIT_ERROR

    log.info("Correlating characteristic sets with schema")
    result = correlate_pg(summary, schema)
    multi_label = _run_multi_label_probe(client, summary, args)
    scan_results = _run_scan_if_requested(client, result, args)
    if scan_results is not None:
        result = apply_scan(result, scan_results, summary=summary, multi_label=multi_label)
    log.info("Rendering report")

    if args.json:
        payload = pg_report_payload(
            summary,
            schema,
            result,
            endpoint=client.base_url,
            multi_label=multi_label,
            scan=scan_results,
            statistics=statistics,
        )
        return _emit_json(payload)

    sys.stdout.write(
        render_pg_report(
            summary,
            schema,
            result,
            endpoint=client.base_url,
            multi_label=multi_label,
            scan=scan_results,
            statistics=statistics,
            details=args.details,
        )
    )
    return EXIT_OK


def _run_pg_fallback(
    client: NeptuneClient,
    summary: PGSummary,
    args: argparse.Namespace,
) -> int:
    """Fallback flow when ``neptune.graph.pg_schema`` is unavailable."""
    log.warning(
        "pg_schema unavailable (Neptune < 1.4.8.0). Falling back to per-label count queries."
    )
    node_labels = list(summary.node_labels)
    edge_labels = list(summary.edge_labels)
    log.info(
        "Querying %d node label(s) + %d edge label(s) directly ...",
        len(node_labels),
        len(edge_labels),
    )
    fallback = fetch_label_counts(client, node_labels, edge_labels)

    multi_label = _run_multi_label_probe(client, summary, args)

    if args.json:
        payload = pg_fallback_report_payload(
            summary,
            fallback,
            endpoint=client.base_url,
            multi_label=multi_label,
        )
        return _emit_json(payload)

    sys.stdout.write(
        render_pg_fallback_report(
            summary,
            fallback,
            endpoint=client.base_url,
            multi_label=multi_label,
        )
    )
    return EXIT_OK


def _run_scan_if_requested(
    client: NeptuneClient,
    result: PGCorrelationResult,
    args: argparse.Namespace,
) -> ScanResults | None:
    """Plan + execute the count scan.

    Behavior:

    - ``--api-only``: skip scans entirely; ranges will be shown in the report.
    - Otherwise (default): scan any range-bounded label/edge.
    """
    if args.api_only:
        log.info("Skipping scans (--api-only). Ranges may appear in the report.")
        return None

    plan = plan_scan(result)
    if plan.total_queries == 0:
        log.info("Scan skipped: all bounds are already exact.")
        return None
    log.info(
        "Scanning: %d node query(s) + %d edge query(s)",
        len(plan.node_labels_to_query),
        len(plan.edge_labels_to_query),
    )
    scan = execute_scan(client, plan, result)
    _warn_if_scan_fully_failed(scan)
    return scan


def _warn_if_scan_fully_failed(scan: ScanResults) -> None:
    """Print a clear stderr warning when every planned scan query failed.

    Individual query failures are logged per-label; this catches the
    "scan produced nothing" case that would otherwise leave the user
    wondering why ranges are still in the report.
    """
    attempted = scan.plan.total_queries
    succeeded = len(scan.node_scans) + len(scan.edge_scans)
    if attempted > 0 and succeeded == 0:
        sys.stderr.write(
            f"warning: all {attempted} scan query(s) failed. Ranges preserved "
            "in the report. Check IAM permissions "
            "(neptune-db:ReadDataViaQuery), --timeout, or cluster load. "
            "Use --api-only to skip scans entirely.\n"
        )


def _run_multi_label_probe(
    client: NeptuneClient,
    summary: PGSummary,
    args: argparse.Namespace,
) -> MultiLabelProbeResult | None:
    """Run the pairwise multi-label probe unless the user opted out.

    On failure, logs a warning and returns ``None`` — the correlation report
    still renders, just without the multi-label section.
    """
    if args.skip_multi_label_check:
        log.info("Skipping multi-label probe (--skip-multi-label-check)")
        return None
    labels = list(summary.node_labels)
    if len(set(labels)) < 2:
        return MultiLabelProbeResult(pairs_checked=0, hits=())
    pair_count = len(labels) * (len(labels) - 1) // 2
    log.info("Multi-label probe: checking %d label pair(s)", pair_count)
    try:
        result = probe_multi_label_pairs(client, labels)
    except NeptuneClientError as e:
        log.warning("Multi-label probe failed: %s. Continuing without the check.", e)
        print(
            f"Warning: multi-label probe failed ({e}). "
            "Per-label counts below may be inflated if any nodes carry multiple "
            "labels. Re-run with --skip-multi-label-check to suppress this warning "
            "or investigate the underlying failure.",
            file=sys.stderr,
        )
        return None
    if result.any_multi_label:
        log.info(
            "Multi-label probe: found %d pair(s) with multi-labeled nodes",
            len(result.hits),
        )
    else:
        log.info("Multi-label probe: no multi-labeled nodes found")
    return result


def _dump_pg_raw(client: NeptuneClient, summary: PGSummary, schema: PGSchema) -> None:
    _emit_json(
        {
            "endpoint": client.base_url,
            "mode": GraphMode.PG.value,
            "pg_summary": _to_jsonable(summary),
            "pg_schema": _to_jsonable(schema),
        }
    )
