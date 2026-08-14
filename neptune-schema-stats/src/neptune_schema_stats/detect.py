"""Auto-detect whether a Neptune cluster is property-graph or RDF.

Strategy:
    1. Try the PG summary endpoint. If it returns a valid response (even
       StatisticsNotAvailable is fine — it means the endpoint exists), we
       consider this a PG cluster.
    2. Otherwise try the RDF summary endpoint with the same logic.
    3. If both fail with real errors, surface a combined diagnostic.

Neptune clusters actually support both endpoints simultaneously — a PG-loaded
cluster still responds to /rdf/statistics/summary (usually with empty or minimal
data) and vice versa. So we detect based on which one has meaningful content,
falling back to whichever exposes the endpoint.
"""

from __future__ import annotations

import logging

from neptune_schema_stats.client.base import (
    NeptuneClient,
    NeptuneClientError,
    NeptuneHTTPError,
    NeptuneStatisticsNotAvailableError,
)
from neptune_schema_stats.client.pg_summary import fetch_pg_summary
from neptune_schema_stats.client.rdf_summary import fetch_rdf_summary
from neptune_schema_stats.models import GraphMode, PGSummary, RDFSummary

log = logging.getLogger(__name__)


class ModeDetectionError(NeptuneClientError):
    """Both graph model endpoints failed or returned no meaningful data."""


def detect_mode(client: NeptuneClient) -> GraphMode:
    """Return whichever mode the cluster appears to be using.

    Prefers PG when both endpoints return data. Callers who need to distinguish
    empty from populated should use the more granular ``probe_endpoints``.
    """
    result = probe_endpoints(client)
    if result.pg_summary is not None and result.pg_summary.num_nodes > 0:
        return GraphMode.PG
    if result.rdf_summary is not None and result.rdf_summary.num_quads > 0:
        return GraphMode.RDF
    if result.pg_summary is not None:
        return GraphMode.PG
    if result.rdf_summary is not None:
        return GraphMode.RDF
    raise ModeDetectionError(
        "Could not determine graph mode. Both PG and RDF summary endpoints failed.\n"
        f"  PG error:  {result.pg_error}\n"
        f"  RDF error: {result.rdf_error}\n"
        "Try passing --mode pg or --mode rdf explicitly."
    )


class ProbeResult:
    """The outcome of probing both PG and RDF summary endpoints."""

    __slots__ = ("pg_error", "pg_summary", "rdf_error", "rdf_summary")

    def __init__(
        self,
        pg_summary: PGSummary | None,
        pg_error: Exception | None,
        rdf_summary: RDFSummary | None,
        rdf_error: Exception | None,
    ) -> None:
        self.pg_summary = pg_summary
        self.pg_error = pg_error
        self.rdf_summary = rdf_summary
        self.rdf_error = rdf_error


def probe_endpoints(client: NeptuneClient) -> ProbeResult:
    """Attempt both summary endpoints, returning whichever succeeded.

    ``StatisticsNotAvailableException`` is treated as endpoint-present-but-empty
    and does not populate the corresponding summary field — but does not
    populate the error field either.
    """
    pg_summary: PGSummary | None = None
    pg_error: Exception | None = None
    try:
        pg_summary = fetch_pg_summary(client)
    except NeptuneStatisticsNotAvailableError as e:
        log.debug("PG stats not yet computed: %s", e)
    except NeptuneHTTPError as e:
        pg_error = e
    except NeptuneClientError as e:
        pg_error = e

    rdf_summary: RDFSummary | None = None
    rdf_error: Exception | None = None
    try:
        rdf_summary = fetch_rdf_summary(client)
    except NeptuneStatisticsNotAvailableError as e:
        log.debug("RDF stats not yet computed: %s", e)
    except NeptuneHTTPError as e:
        rdf_error = e
    except NeptuneClientError as e:
        rdf_error = e

    return ProbeResult(pg_summary, pg_error, rdf_summary, rdf_error)
