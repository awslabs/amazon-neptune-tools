"""Minimal openCypher query helpers.

Thin wrappers around :meth:`NeptuneClient.execute_cypher` (which itself goes
through ``boto3.client('neptunedata').execute_open_cypher_query``). This
module exists to consolidate scalar-count extraction — mirrors
:mod:`client.sparql` for symmetry.

Purpose-specific query builders (schema calls, scan counts, multi-label
probes) live with their callers.
"""

from __future__ import annotations

from typing import Any

from neptune_schema_stats.client.base import NeptuneClient


def execute_cypher(client: NeptuneClient, query: str) -> dict[str, Any]:
    """Run an openCypher query, return the raw JSON body.

    :raises NeptuneClientError: on transport or query errors.
    """
    return client.execute_cypher(query)


def execute_cypher_scalar(
    client: NeptuneClient, query: str, *, var: str = "c", default: int = 0
) -> int:
    """Run a ``RETURN count(...) AS <var>``-shaped query and extract the scalar.

    Returns ``default`` (0 by default) if the result envelope is empty or the
    binding is missing or unparseable — an empty label legitimately returns 0.

    Expected result shape: ``{"results": [{"<var>": <int>}]}``.
    """
    body = execute_cypher(client, query)
    results = body.get("results", [])
    if not results:
        return default
    first = results[0]
    if not isinstance(first, dict) or var not in first:
        return default
    try:
        return int(first[var])
    except (TypeError, ValueError):
        return default
