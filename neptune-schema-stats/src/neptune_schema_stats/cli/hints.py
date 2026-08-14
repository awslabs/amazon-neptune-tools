"""Actionable hints and status printers for the CLI.

Every user-facing exit path in the tool is expected to print something
useful to stderr — this module centralizes the "what went wrong and what
you can do about it" formatting.
"""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime

from neptune_schema_stats.cli._constants import EXIT_ERROR, EXIT_STATS_UNAVAILABLE
from neptune_schema_stats.client import NeptuneClient, NeptuneClientError, NeptuneHTTPError
from neptune_schema_stats.client.statistics import (
    PG_STATISTICS_PATH,
    StatisticsInfo,
    fetch_statistics,
    is_statistics_unavailable_error,
)
from neptune_schema_stats.models import PGSchema, SchemaState

log = logging.getLogger(__name__)


def _hint_for_http_error(err: NeptuneHTTPError) -> str | None:
    """Return an actionable hint for well-known Neptune error patterns."""
    api_err = err.error
    code = (api_err.code or "").lower()
    msg = (api_err.detailed_message or "").lower()

    if "malformedqueryexception" in code and "pg_schema" in msg:
        return (
            "The neptune.graph.pg_schema procedure is only available on Neptune "
            "engine 1.4.8.0 or later. Upgrade your cluster to use "
            "--refresh/--refresh, or omit them and use the "
            "summary API only."
        )
    if "malformedqueryexception" in code:
        return (
            "Neptune rejected the query as malformed. This is likely a bug "
            "in this tool if it happened during a scan or probe query. "
            "Re-run with -v for the full query text."
        )
    if "statisticscomputationfailedexception" in code or (
        "statisticsnotavailable" in code and "failed" in msg
    ):
        return (
            "DFE statistics computation failed on the cluster (often a "
            "resource limit or an internal error). Wait for the cluster to "
            "recover and retry, or contact AWS support with the requestId in "
            "the error message."
        )
    if "accessdeniedexception" in code or "notauthorized" in code:
        return (
            "The current IAM identity lacks permission for this operation. "
            "This tool needs: neptune-db:GetGraphSummary (always), plus "
            "neptune-db:ReadDataViaQuery for pg_schema, multi-label probe, "
            "class-count probe, and scans."
        )
    if "throttlingexception" in code:
        return (
            "The cluster is throttling requests. Retry after a short delay, "
            "or use --api-only to skip scans/probes entirely."
        )
    if "querylimitexceededexception" in code or ("querytimeoutexception" in code):
        return (
            "A scan query exceeded a cluster query limit or timeout. Raise "
            "the cluster's neptune_query_timeout parameter, raise --timeout, "
            "or use --api-only to skip scans and accept ranges."
        )
    if "readonlyviolationexception" in code:
        return (
            "The endpoint is a read-only replica. --refresh/--refresh require the writer endpoint."
        )
    if "internalfailureexception" in code:
        return (
            "Neptune returned an internal error. Retry after a delay; if the "
            "error persists, contact AWS support with the requestId."
        )
    if "invalidparameterexception" in code:
        return (
            "One of the request parameters was rejected by Neptune. If this "
            "happened on the summary or schema endpoint, re-run with -v for "
            "the URL and check your endpoint host/port."
        )
    return None


def _handle_http_error(err: NeptuneHTTPError) -> int:
    print(f"Neptune API error: {err}", file=sys.stderr)
    hint = _hint_for_http_error(err)
    if hint:
        print(f"Hint: {hint}", file=sys.stderr)
    return EXIT_ERROR


def _print_schema_status(schema: PGSchema, *, prefix: str = "schema") -> None:
    """Print a one-line human-readable schema status to stderr."""
    ts = datetime.now(UTC).astimezone().strftime("%H:%M:%S")
    state = schema.state().value
    pct = schema.status.progress_percentage or "0"
    err_suffix = f" — {schema.status.error_message}" if schema.status.error_message else ""
    print(f"[{ts}] {prefix}: {state} ({pct}%){err_suffix}", file=sys.stderr)


def _print_statistics_status(stats: StatisticsInfo, *, prefix: str = "stats") -> None:
    """One-line stats-status log used while polling for a refresh to complete."""
    ts = datetime.now(UTC).astimezone().strftime("%H:%M:%S")
    print(
        f"[{ts}] {prefix}: id={stats.statistics_id} date={stats.date}",
        file=sys.stderr,
    )


def _fetch_statistics_best_effort(
    client: NeptuneClient, path: str = PG_STATISTICS_PATH
) -> StatisticsInfo | None:
    """Fetch DFE statistics status from ``path``; returns ``None`` if the
    endpoint isn't exposed on this engine (older Neptune versions) or the
    data model isn't loaded. Any other error also degrades to ``None`` with
    a warning — statistics info is informational only and shouldn't fail
    the report."""
    try:
        return fetch_statistics(client, path)
    except NeptuneHTTPError as exc:
        if is_statistics_unavailable_error(exc):
            log.debug("%s endpoint not available on this engine.", path)
        else:
            log.warning("Could not fetch %s: %s", path, exc)
        return None
    except NeptuneClientError as exc:
        log.warning("Malformed statistics response for %s: %s", path, exc)
        return None


def _print_statistics_limit_reached_hint(stats: StatisticsInfo, path: str) -> None:
    """Print an actionable message when the DFE statistics engine reports
    ``active: false`` with a ``"Limit reached"`` note.

    This is a persistent state — Neptune has more characteristic sets /
    property signatures than the DFE can index, and neither ``--refresh``
    nor waiting will resolve it. The remediation is on the AWS side.
    """
    note = stats.note or "Statistics inactive."
    print(
        f"Neptune DFE statistics are not usable ({path}):\n"
        f"    {note}\n"
        "\n"
        "This indicates the cluster has hit a DFE statistics limit — the\n"
        "graph has more property/label signatures than the DFE engine can\n"
        "index. The Graph Summary API (which this tool relies on) will not\n"
        "produce reliable output until the limit is resolved.\n"
        "\n"
        "Neither --refresh nor waiting will resolve this on its own. Actions:\n"
        "\n"
        "  1. Review the limit documented at:\n"
        "       https://docs.aws.amazon.com/neptune/latest/userguide/neptune-dfe-statistics.html\n"
        "  2. If reducing data model cardinality (fewer distinct labels /\n"
        "     properties / characteristic sets) isn't feasible, open an\n"
        "     AWS Support case referencing 'Neptune DFE statistics limit'\n"
        "     so the service team can review the cluster's configuration.\n",
        file=sys.stderr,
    )


def _check_statistics_limit(stats: StatisticsInfo | None, path: str) -> int | None:
    """Return an exit code if ``stats`` shows the DFE hit its limit (and
    print the guidance hint as a side effect). Return ``None`` when
    statistics look usable or there's no snapshot to check."""
    if stats is None:
        return None
    if stats.has_limit_note:
        _print_statistics_limit_reached_hint(stats, path)
        return EXIT_STATS_UNAVAILABLE
    if not stats.active:
        # active=false without a limit note: less common, but still means
        # the summary API can't be trusted. Surface generically.
        note = stats.note or "no additional detail provided by Neptune"
        print(
            f"Neptune DFE statistics at {path} are inactive ({note}).\n"
            "The Graph Summary API may not return reliable data. Re-run with\n"
            "--refresh to trigger a manual recomputation.",
            file=sys.stderr,
        )
        return EXIT_STATS_UNAVAILABLE
    return None


def _print_schema_not_usable_hint(schema: PGSchema, base_url: str) -> None:
    state = schema.state().value
    err_msg = schema.status.error_message
    progress = schema.status.progress_percentage or "0"

    if schema.state() is SchemaState.FAILED:
        detail = f"\n    error: {err_msg}" if err_msg else ""
        print(
            f"PG schema state is Failed.{detail}\n"
            "\n"
            "The schema computation failed on the cluster. Common causes:\n"
            "  - Resource limits exceeded (too many labels/properties)\n"
            "  - Cluster restart mid-compute\n"
            "  - Insufficient DFE memory\n"
            "\n"
            "Retry with:\n"
            f"    neptune-schema-stats --endpoint <host> --iam --region <region> \\\n"
            "        --refresh",
            file=sys.stderr,
        )
        return

    if schema.state() is SchemaState.IN_PROGRESS:
        print(
            f"PG schema is still computing ({progress}% complete). Wait for "
            "completion with:\n"
            f"    neptune-schema-stats --endpoint <host> --iam --region <region> \\\n"
            "        --refresh",
            file=sys.stderr,
        )
        return

    print(
        f"PG schema is not usable (state: {state}). Per-label statistics require "
        "a Completed schema.\n"
        "\n"
        "Trigger and wait for the schema to compute:\n"
        f"    neptune-schema-stats --endpoint <host> --iam --region <region> \\\n"
        "        --refresh\n"
        "\n"
        "Or dump raw API data without correlation:\n"
        f"    neptune-schema-stats --endpoint <host> --iam --region <region> --dump\n"
        "\n"
        "Note: the pg_schema procedure requires Neptune engine 1.4.8.0 or later.",
        file=sys.stderr,
    )
