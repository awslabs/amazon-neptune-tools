"""Neptune DFE statistics management, mode-agnostic.

The Graph Summary API (both PG and RDF variants) is derived from
Neptune's statistics engine. When statistics are stale — or, for a freshly
loaded cluster, not yet computed at all — the summary is unavailable or
lags reality. This module provides:

- ``fetch_statistics(client, path)`` — read the current statistics snapshot
- ``trigger_statistics_refresh(client, path)`` — request a manual refresh
- ``wait_for_statistics_refresh(client, path, initial, …)`` — block until
  the ``statistics_id`` changes (indicating the refresh completed) or a
  timeout elapses

Backed by ``boto3.client('neptunedata')``:

    /pg/statistics       → GetPropertygraphStatistics / ManagePropertygraphStatistics
    /rdf/statistics      → GetSparqlStatistics / ManageSparqlStatistics

Both endpoints return the same payload shape, so a single
:class:`StatisticsInfo` dataclass covers both.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from neptune_schema_stats.client.base import (
    NeptuneClient,
    NeptuneClientError,
    NeptuneHTTPError,
)

# Path constants remain as discriminators (used by callers to select PG vs RDF).
PG_STATISTICS_PATH = "/pg/statistics"
RDF_STATISTICS_PATH = "/rdf/statistics"


@dataclass(frozen=True, slots=True)
class StatisticsInfo:
    """Snapshot of Neptune's DFE statistics status for a given data model."""

    active: bool
    """``True`` if statistics are active and being used by the query engine.

    When ``False``, statistics exist but can't be used. Check :attr:`note`
    for the reason — the most common one is that the DFE hit its documented
    characteristic-set / signature limit and refuses to serve the summary
    API until the operator resolves it (typically via a support case)."""

    auto_compute: bool
    """``True`` if Neptune auto-recomputes statistics as data changes."""

    statistics_id: int | None
    """Monotonic identifier that changes each time statistics are recomputed.
    Used to detect completion of a manual refresh."""

    date: str | None
    """ISO-8601 timestamp of the last computation, e.g.
    ``"2026-04-04T23:10:45.114000+00:00"``. ``None`` if never computed."""

    signature_count: int | None
    """Number of distinct property/label signatures observed."""

    instance_count: int | None
    """Number of node/edge instances covered by statistics."""

    predicate_count: int | None
    """Number of distinct predicates observed."""

    note: str | None = None
    """Optional free-form status note from Neptune. When statistics are
    inactive because the DFE hit a limit, Neptune populates this with a
    human-readable explanation like ``"Limit reached: Statistics are not
    available"``. Use :attr:`has_limit_note` to detect that specific case."""

    @property
    def has_limit_note(self) -> bool:
        """Return ``True`` when the note explicitly mentions a DFE limit.
        The exact wording is Neptune-controlled — we match loosely."""
        if not self.note:
            return False
        note = self.note.lower()
        return "limit" in note and ("reached" in note or "exceeded" in note)

    @property
    def is_usable(self) -> bool:
        """Return ``True`` when statistics are both active AND lack a
        limit-reached note. This is the ``go/no-go`` signal callers should
        check before assuming the summary API will work."""
        return self.active and not self.has_limit_note


def fetch_statistics(client: NeptuneClient, path: str) -> StatisticsInfo:
    """Fetch the statistics status for the model identified by ``path``.

    ``path`` acts purely as a discriminator (``/pg/statistics`` or
    ``/rdf/statistics``) — the actual call goes through boto3 neptunedata.

    :raises NeptuneClientError: on malformed responses.
    :raises NeptuneHTTPError: on transport/HTTP-level failures. The
        endpoint may not be exposed on very old Neptune engines; callers
        can catch and skip.
    """
    body = _get_statistics(client, path)
    payload = body.get("payload", {})
    if not isinstance(payload, dict):
        raise NeptuneClientError(
            f"Malformed statistics response from {path}: 'payload' is not an object."
        )
    sig = payload.get("signatureInfo", {}) or {}
    try:
        return StatisticsInfo(
            active=bool(payload.get("active", False)),
            auto_compute=bool(payload.get("autoCompute", False)),
            statistics_id=_maybe_int(payload.get("statisticsId")),
            date=_maybe_iso(payload.get("date")),
            signature_count=_maybe_int(sig.get("signatureCount")),
            instance_count=_maybe_int(sig.get("instanceCount")),
            predicate_count=_maybe_int(sig.get("predicateCount")),
            note=_maybe_str(payload.get("note")),
        )
    except (TypeError, ValueError) as exc:
        raise NeptuneClientError(f"Malformed statistics response from {path}: {exc}") from exc


def trigger_statistics_refresh(client: NeptuneClient, path: str) -> dict[str, Any]:
    """Trigger a manual statistics recomputation for the model at ``path``.

    Asynchronous: the response confirms Neptune accepted the request but the
    recomputation runs in the background. Use
    :func:`wait_for_statistics_refresh` to block until it completes.
    """
    if path == PG_STATISTICS_PATH:
        return client.refresh_pg_statistics()
    if path == RDF_STATISTICS_PATH:
        return client.refresh_rdf_statistics()
    raise ValueError(f"Unknown statistics path: {path!r}")


def wait_for_statistics_refresh(
    client: NeptuneClient,
    path: str,
    *,
    initial: StatisticsInfo,
    poll_interval: float = 5.0,
    timeout: float = 600.0,
    on_poll: Callable[[StatisticsInfo], None] | None = None,
) -> StatisticsInfo:
    """Poll the statistics endpoint at ``path`` until ``statistics_id``
    changes from ``initial.statistics_id``.

    :param initial: snapshot taken before the refresh was triggered.
    :param poll_interval: seconds between polls.
    :param timeout: maximum seconds to wait.
    :param on_poll: optional callback invoked after every poll with the
        latest snapshot; useful for progress logging.
    :raises TimeoutError: if the id doesn't change within ``timeout``.
    """
    deadline = time.monotonic() + timeout
    while True:
        current = fetch_statistics(client, path)
        if on_poll is not None:
            on_poll(current)
        if current.statistics_id != initial.statistics_id:
            return current
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Statistics at {path} did not refresh within {timeout:.0f}s "
                f"(statistics_id still {initial.statistics_id})."
            )
        time.sleep(poll_interval)


def is_statistics_unavailable_error(exc: Exception) -> bool:
    """Return True if ``exc`` indicates the statistics endpoint isn't
    available on this Neptune engine (older versions) or for this data
    model (e.g. querying RDF stats on a cluster with no RDF data loaded).

    Neptune returns a 404-style ``EndpointNotFoundException`` or a
    ``MalformedQueryException`` mentioning the path in that case.
    """
    if not isinstance(exc, NeptuneHTTPError):
        return False
    api_error = getattr(exc, "error", None)
    if api_error is None:
        return False
    code = (api_error.code or "").lower()
    detail = (api_error.detailed_message or "").lower()
    if "endpointnotfound" in code:
        return True
    return ("statistics" in detail) and ("not found" in detail or "unknown" in detail)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _get_statistics(client: NeptuneClient, path: str) -> dict[str, Any]:
    if path == PG_STATISTICS_PATH:
        return client.get_pg_statistics()
    if path == RDF_STATISTICS_PATH:
        return client.get_rdf_statistics()
    raise ValueError(f"Unknown statistics path: {path!r}")


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _maybe_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _maybe_iso(value: Any) -> str | None:
    """Convert a datetime to ISO-8601, or coerce any other value to str.

    boto3 parses ``SyntheticTimestamp_date_time`` response fields into
    ``datetime.datetime``. Normalize to an ISO-8601 string so downstream
    comparisons and formatting are stable regardless of Neptune version.
    """
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
