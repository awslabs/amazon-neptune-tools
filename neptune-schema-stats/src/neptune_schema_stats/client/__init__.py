"""Client subpackage for Neptune's HTTP APIs."""

from neptune_schema_stats.client.base import (
    IAMAuthConfig,
    NeptuneClient,
    NeptuneClientError,
    NeptuneHTTPError,
    NeptuneStatisticsNotAvailableError,
)
from neptune_schema_stats.client.pg_schema import (
    fetch_pg_schema,
    trigger_pg_schema_compute,
    wait_for_schema,
)
from neptune_schema_stats.client.pg_summary import fetch_pg_summary
from neptune_schema_stats.client.rdf_summary import fetch_rdf_summary

__all__ = [
    "IAMAuthConfig",
    "NeptuneClient",
    "NeptuneClientError",
    "NeptuneHTTPError",
    "NeptuneStatisticsNotAvailableError",
    "fetch_pg_schema",
    "fetch_pg_summary",
    "fetch_rdf_summary",
    "trigger_pg_schema_compute",
    "wait_for_schema",
]
