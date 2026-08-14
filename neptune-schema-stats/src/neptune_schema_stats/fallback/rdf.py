"""SPARQL-only fallback for RDF reports.

When the DFE statistics engine can't serve the RDF Graph Summary API — for
example on a cluster that has hit its statistics limit — we can still
answer a handful of useful questions by issuing bounded SPARQL aggregate
queries directly. This module is the "we can't rely on the summary API,
what CAN we tell the user" implementation for RDF, analogous to
:mod:`neptune_schema_stats.fallback.pg` on the PG side.

Coverage:

- Total triple count (``SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }``)
- Distinct subject count
- Distinct predicate count
- Class distribution (reuses :func:`sparql_class_counts`)

Not covered: characteristic-set analysis, per-predicate breakdown, subject
typing — these would require full-graph traversal or many separate queries,
and on a cluster where DFE has already given up they're likely to be
unusable in practice.

The fallback is triggered from the CLI (``_run_rdf_default``) when a stats
limit is detected and ``--api-only`` was NOT passed. ``--api-only``
preserves "no I/O beyond metadata APIs" semantics and exits with a
descriptive error instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from neptune_schema_stats.client.base import NeptuneClient, NeptuneClientError
from neptune_schema_stats.client.sparql import (
    sparql_class_counts,
    sparql_count_distinct_predicates,
    sparql_count_distinct_subjects,
    sparql_count_triples,
)


@dataclass(frozen=True, slots=True)
class RDFFallbackResult:
    """Aggregate metrics fetched via SPARQL when the summary API is not usable.

    Any individual query that fails records the exception message in
    ``failed_queries`` so the report can note which measurements were lost.
    The tool degrades gracefully — one failing query does not abort the rest.
    """

    total_triples: int | None = None
    distinct_subjects: int | None = None
    distinct_predicates: int | None = None
    class_counts: dict[str, int] = field(default_factory=dict)
    failed_queries: tuple[str, ...] = ()


def fetch_rdf_fallback(client: NeptuneClient) -> RDFFallbackResult:
    """Run the four fallback SPARQL queries and return the aggregate result."""
    failed: list[str] = []

    def _try(name: str, fn):
        try:
            return fn()
        except NeptuneClientError as exc:
            failed.append(f"{name}: {exc}")
            return None

    total = _try("count triples", lambda: sparql_count_triples(client))
    subjects = _try("count distinct subjects", lambda: sparql_count_distinct_subjects(client))
    predicates = _try("count distinct predicates", lambda: sparql_count_distinct_predicates(client))
    class_counts = _try("class counts", lambda: sparql_class_counts(client)) or {}

    return RDFFallbackResult(
        total_triples=total,
        distinct_subjects=subjects,
        distinct_predicates=predicates,
        class_counts=class_counts,
        failed_queries=tuple(failed),
    )
