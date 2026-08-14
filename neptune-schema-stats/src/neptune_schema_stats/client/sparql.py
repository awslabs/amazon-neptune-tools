"""Minimal SPARQL query helpers for RDF-mode Neptune endpoints.

Uses raw HTTP via :meth:`NeptuneClient.sparql_query` because the
``neptunedata`` SDK doesn't expose an arbitrary-SPARQL operation.

Currently focused on the class-count probe used by the RDF default report
plus scalar aggregate helpers used by the RDF fallback.
"""

from __future__ import annotations

from typing import Any

from neptune_schema_stats.client.base import NeptuneClient


def sparql_class_counts(client: NeptuneClient) -> dict[str, int]:
    """Return per-class typed-subject counts via a single SPARQL query.

    :returns: mapping of class URI -> subject count. Empty dict if the graph
        has no typed subjects.
    :raises NeptuneClientError: on transport or query errors.
    """
    query = (
        "SELECT ?cls (COUNT(?s) AS ?c) "
        "WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?cls } "
        "GROUP BY ?cls"
    )
    return _parse_class_count_bindings(client.sparql_query(query))


def sparql_count_triples(client: NeptuneClient) -> int:
    """Return the total number of triples via ``SELECT (COUNT(*) AS ?c)``."""
    body = client.sparql_query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }")
    return _parse_scalar_count_binding(body, var="c")


def sparql_count_distinct_subjects(client: NeptuneClient) -> int:
    """Return the number of distinct subjects via
    ``SELECT (COUNT(DISTINCT ?s) AS ?c)``."""
    body = client.sparql_query("SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o }")
    return _parse_scalar_count_binding(body, var="c")


def sparql_count_distinct_predicates(client: NeptuneClient) -> int:
    """Return the number of distinct predicates via
    ``SELECT (COUNT(DISTINCT ?p) AS ?c)``."""
    body = client.sparql_query("SELECT (COUNT(DISTINCT ?p) AS ?c) WHERE { ?s ?p ?o }")
    return _parse_scalar_count_binding(body, var="c")


def _parse_scalar_count_binding(body: dict[str, Any], *, var: str) -> int:
    """Extract a single scalar count from a SPARQL COUNT(*)/COUNT(DISTINCT ?x)
    result. Returns 0 if the binding is missing or unparseable — a
    completely empty graph legitimately returns 0."""
    results = body.get("results", {}).get("bindings", [])
    if not results:
        return 0
    first = results[0]
    b = first.get(var) if isinstance(first, dict) else None
    if not b:
        return 0
    raw = b.get("value")
    if raw is None:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _parse_class_count_bindings(body: dict[str, Any]) -> dict[str, int]:
    """Parse standard SPARQL SELECT JSON results into class-URI -> count."""
    results = body.get("results", {}).get("bindings", [])
    out: dict[str, int] = {}
    for binding in results:
        cls_binding = binding.get("cls")
        c_binding = binding.get("c")
        if not cls_binding or not c_binding:
            continue
        uri = cls_binding.get("value")
        raw_count = c_binding.get("value")
        if uri is None or raw_count is None:
            continue
        try:
            out[str(uri)] = int(raw_count)
        except (TypeError, ValueError):
            continue
    return out
