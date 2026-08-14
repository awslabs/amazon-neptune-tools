"""Client for Neptune's Property Graph summary API.

Uses ``boto3.client('neptunedata').get_propertygraph_summary(mode='detailed')``.
Docs: https://docs.aws.amazon.com/neptune/latest/userguide/neptune-graph-summary.html
"""

from __future__ import annotations

from neptune_schema_stats.client.base import NeptuneClient, NeptuneClientError
from neptune_schema_stats.models import PGSummary


def fetch_pg_summary(client: NeptuneClient) -> PGSummary:
    """Fetch and parse the PG graph summary in detailed mode.

    :raises NeptuneStatisticsNotAvailableError: DFE statistics have not been computed.
    :raises NeptuneHTTPError: Any other Neptune-side error.
    :raises NeptuneClientError: Network / parsing failure.
    """
    body = client.get_pg_summary()
    try:
        return PGSummary.from_json(body)
    except (KeyError, TypeError, ValueError) as exc:
        raise NeptuneClientError(f"Malformed PG summary response: {exc}") from exc
