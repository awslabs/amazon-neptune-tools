"""Fallback path for clusters without ``neptune.graph.pg_schema`` (< 1.4.8.0).

When pg_schema is unavailable, we can still produce a useful report by
querying node/edge label counts directly. This module handles that fallback
flow — it fetches per-label exact counts and renders a simplified report
that omits schema-dependent columns (per-label properties, source→target
labels, characteristic-set correlation).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from neptune_schema_stats.client.base import NeptuneClient, NeptuneClientError
from neptune_schema_stats.scan import query_edge_label_count, query_node_label_count

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FallbackResult:
    """Exact per-label node/edge counts from a fallback query pass.

    Populated for graphs where ``neptune.graph.pg_schema`` is unavailable.
    """

    node_counts: dict[str, int]
    edge_counts: dict[str, int]
    failed_node_labels: tuple[str, ...] = ()
    failed_edge_labels: tuple[str, ...] = ()

    @property
    def total_queries(self) -> int:
        return (
            len(self.node_counts)
            + len(self.edge_counts)
            + len(self.failed_node_labels)
            + len(self.failed_edge_labels)
        )


def fetch_label_counts(
    client: NeptuneClient,
    node_labels: list[str],
    edge_labels: list[str],
) -> FallbackResult:
    """Query per-label counts for every node label and edge label.

    Individual failures are logged and captured in ``failed_*_labels`` so a
    partial result still renders.
    """
    node_counts: dict[str, int] = {}
    edge_counts: dict[str, int] = {}
    failed_nodes: list[str] = []
    failed_edges: list[str] = []

    for label in node_labels:
        try:
            node_counts[label] = query_node_label_count(client, label)
        except NeptuneClientError as exc:
            log.warning("Fallback node count failed for %s: %s", label, exc)
            failed_nodes.append(label)

    for label in edge_labels:
        try:
            edge_counts[label] = query_edge_label_count(client, label)
        except NeptuneClientError as exc:
            log.warning("Fallback edge count failed for %s: %s", label, exc)
            failed_edges.append(label)

    return FallbackResult(
        node_counts=node_counts,
        edge_counts=edge_counts,
        failed_node_labels=tuple(failed_nodes),
        failed_edge_labels=tuple(failed_edges),
    )


def is_pg_schema_unavailable_error(exc: Exception) -> bool:
    """Detect the ``MalformedQueryException`` Neptune returns when the
    ``neptune.graph.pg_schema`` procedure is unknown (< 1.4.8.0)."""
    from neptune_schema_stats.client.base import NeptuneHTTPError

    if not isinstance(exc, NeptuneHTTPError):
        return False
    code = (exc.error.code or "").lower()
    msg = (exc.error.detailed_message or "").lower()
    return "malformedqueryexception" in code and "pg_schema" in msg
