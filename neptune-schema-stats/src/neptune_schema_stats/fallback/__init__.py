"""Fallback modules for degraded-mode operation.

- ``pg`` — per-label ``count(*)`` queries when ``neptune.graph.pg_schema`` is unavailable
- ``rdf`` — SPARQL aggregate queries when DFE statistics are unusable
"""

from neptune_schema_stats.fallback.pg import (
    FallbackResult,
    fetch_label_counts,
    is_pg_schema_unavailable_error,
)
from neptune_schema_stats.fallback.rdf import RDFFallbackResult, fetch_rdf_fallback

__all__ = [
    "FallbackResult",
    "RDFFallbackResult",
    "fetch_label_counts",
    "fetch_rdf_fallback",
    "is_pg_schema_unavailable_error",
]
