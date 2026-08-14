"""Client for Neptune's RDF summary API.

Uses ``boto3.client('neptunedata').get_rdf_graph_summary(mode='detailed')``.
Docs: https://docs.aws.amazon.com/neptune/latest/userguide/neptune-graph-summary.html
"""

from __future__ import annotations

from neptune_schema_stats.client.base import NeptuneClient, NeptuneClientError
from neptune_schema_stats.models import RDFSummary


def fetch_rdf_summary(client: NeptuneClient) -> RDFSummary:
    """Fetch and parse the RDF graph summary in detailed mode.

    :raises NeptuneStatisticsNotAvailableError: DFE statistics have not been computed.
    :raises NeptuneHTTPError: Any other Neptune-side error.
    :raises NeptuneClientError: Network / parsing failure.
    """
    body = client.get_rdf_summary()
    try:
        return RDFSummary.from_json(body)
    except (KeyError, TypeError, ValueError) as exc:
        raise NeptuneClientError(f"Malformed RDF summary response: {exc}") from exc
