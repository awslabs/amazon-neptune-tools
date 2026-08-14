"""Pure correlation logic for RDF summaries.

Analyzes an :class:`RDFSummary` to derive:

- Typed vs untyped subject counts (based on presence of ``rdf:type`` in the
  subject characteristic set)
- Predicate categorization by well-known W3C namespace (``rdf:*``,
  ``rdfs:*``, ``owl:*``, ``skos:*``, everything else as ``custom``)
- Subject characteristic-set analysis with typed/untyped classification
- Optional per-class subject counts (when the SPARQL class-count probe has
  been run)

Unlike PG, RDF has no separate schema API — the "schema" is implicit
(via ``rdf:type`` triples). Property classification into datatype-property
vs object-property is not attempted; the URI shape doesn't reliably
distinguish them.

This module is deliberately pure and I/O-free.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from neptune_schema_stats.models import RDF_TYPE_URI, RDFSubjectStructure, RDFSummary

# Well-known RDF/RDFS/OWL namespace prefixes for predicate categorization.
NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_RDFS = "http://www.w3.org/2000/01/rdf-schema#"
NS_OWL = "http://www.w3.org/2002/07/owl#"
NS_XSD = "http://www.w3.org/2001/XMLSchema#"
NS_SKOS = "http://www.w3.org/2004/02/skos/core#"


class PredicateCategory(StrEnum):
    """How a predicate URI is classified for reporting."""

    RDF = "rdf"
    """RDF built-ins (``rdf:type``, ``rdf:first``, etc.)."""

    RDFS = "rdfs"
    """RDF Schema built-ins (``rdfs:label``, ``rdfs:comment``, etc.)."""

    OWL = "owl"
    """OWL built-ins."""

    SKOS = "skos"
    """SKOS built-ins."""

    CUSTOM = "custom"
    """Everything else — user-defined domain predicates. Whether these are
    data properties or object properties is not derivable from the URI."""


@dataclass(frozen=True, slots=True)
class PredicateStats:
    """Per-predicate statistics from an :class:`RDFSummary`.

    ``occurrence_count`` comes directly from the summary's ``predicates`` field
    (already exact — every predicate URI is counted precisely). ``category``
    is inferred from the URI namespace.
    """

    uri: str
    occurrence_count: int
    category: PredicateCategory

    @property
    def local_name(self) -> str:
        """Local name after the last ``#`` or ``/``."""
        for sep in ("#", "/"):
            if sep in self.uri:
                return self.uri.rsplit(sep, 1)[1]
        return self.uri


@dataclass(frozen=True, slots=True)
class SubjectStructureAnalysis:
    """A single subject characteristic set augmented with typed/untyped classification."""

    structure: RDFSubjectStructure
    is_typed: bool
    """Whether ``rdf:type`` is in the predicate set (subject is typed)."""

    predicate_categories: dict[PredicateCategory, int]
    """Count of predicates in each category for this structure."""

    @property
    def count(self) -> int:
        return self.structure.count

    @property
    def predicate_count(self) -> int:
        return len(self.structure.predicates)


@dataclass(frozen=True, slots=True)
class ClassCount:
    """Exact per-class subject count from a SPARQL class-count probe.

    Uses the query::

        SELECT ?cls (COUNT(?s) AS ?c) WHERE { ?s a ?cls } GROUP BY ?cls

    Because Neptune's edge-type/predicate index covers ``rdf:type``, this
    query is proportional to the number of typed subjects and typically runs
    in seconds even on graphs with tens of millions of triples.
    """

    class_uri: str
    subject_count: int


@dataclass(frozen=True, slots=True)
class RDFCorrelationResult:
    """Complete correlation output for an RDF summary.

    Fields prefixed ``num_`` come directly from the summary API and are exact
    to the moment the summary was computed. Derived fields (typed counts,
    per-category totals) are computed by summing over the summary's own data.
    """

    predicate_stats: tuple[PredicateStats, ...]
    subject_structures: tuple[SubjectStructureAnalysis, ...]
    class_counts: tuple[ClassCount, ...] = ()
    """Per-class subject counts. Empty when the class-count probe wasn't run."""

    # Derived aggregates ----------------------------------------------------

    @property
    def num_typed_subjects(self) -> int:
        return sum(s.count for s in self.subject_structures if s.is_typed)

    @property
    def num_untyped_subjects(self) -> int:
        return sum(s.count for s in self.subject_structures if not s.is_typed)

    @property
    def num_typed_subjects_by_class(self) -> int:
        """Sum of subjects counted per class. Should equal ``num_typed_subjects``
        when the class-count probe covered all types (a small residual may
        indicate multi-typed subjects — one subject with two classes appears
        twice in the sum but only once in ``num_typed_subjects``)."""
        return sum(cc.subject_count for cc in self.class_counts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def correlate_rdf(
    summary: RDFSummary,
    *,
    class_counts: dict[str, int] | None = None,
) -> RDFCorrelationResult:
    """Correlate an RDF summary into a structured result.

    :param summary: parsed :class:`RDFSummary`
    :param class_counts: optional exact per-class subject counts, typically
        obtained via :func:`sparql_class_counts` from the SPARQL client.
    """
    predicate_stats = tuple(
        PredicateStats(
            uri=uri,
            occurrence_count=count,
            category=classify_predicate(uri),
        )
        for uri, count in sorted(
            summary.predicates.items(),
            key=lambda pair: (-pair[1], pair[0]),
        )
    )

    subject_structures = tuple(_analyze_structure(s) for s in summary.subject_structures)

    cc_tuple: tuple[ClassCount, ...] = ()
    if class_counts:
        cc_tuple = tuple(
            ClassCount(class_uri=uri, subject_count=count)
            for uri, count in sorted(
                class_counts.items(),
                key=lambda pair: (-pair[1], pair[0]),
            )
        )

    return RDFCorrelationResult(
        predicate_stats=predicate_stats,
        subject_structures=subject_structures,
        class_counts=cc_tuple,
    )


def classify_predicate(uri: str) -> PredicateCategory:
    """Classify a predicate URI into its namespace category.

    Only recognizes the well-known W3C vocabularies. Everything else falls
    into ``CUSTOM``. Whether a custom predicate is a data property or object
    property is not inferable from its URI.
    """
    if uri.startswith(NS_RDF):
        return PredicateCategory.RDF
    if uri.startswith(NS_RDFS):
        return PredicateCategory.RDFS
    if uri.startswith(NS_OWL):
        return PredicateCategory.OWL
    if uri.startswith(NS_SKOS):
        return PredicateCategory.SKOS
    return PredicateCategory.CUSTOM


def result_to_jsonable(result: RDFCorrelationResult) -> dict[str, Any]:
    """Convert an :class:`RDFCorrelationResult` to a plain nested dict for ``json.dumps``."""
    return {
        "predicate_stats": [
            {
                "uri": ps.uri,
                "local_name": ps.local_name,
                "occurrence_count": ps.occurrence_count,
                "category": ps.category.value,
            }
            for ps in result.predicate_stats
        ],
        "subject_structures": [
            {
                "count": s.structure.count,
                "predicates": list(s.structure.predicates),
                "is_typed": s.is_typed,
                "predicate_categories": {cat.value: n for cat, n in s.predicate_categories.items()},
            }
            for s in result.subject_structures
        ],
        "class_counts": [
            {"class_uri": cc.class_uri, "subject_count": cc.subject_count}
            for cc in result.class_counts
        ],
        "totals": {
            "num_typed_subjects": result.num_typed_subjects,
            "num_untyped_subjects": result.num_untyped_subjects,
            "num_typed_subjects_by_class": result.num_typed_subjects_by_class,
        },
    }


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _analyze_structure(structure: RDFSubjectStructure) -> SubjectStructureAnalysis:
    is_typed = structure.has_rdf_type()

    categories: dict[PredicateCategory, int] = defaultdict(int)
    for pred in structure.predicates:
        categories[classify_predicate(pred)] += 1

    return SubjectStructureAnalysis(
        structure=structure,
        is_typed=is_typed,
        predicate_categories=dict(categories),
    )


# Silence unused-import complaints if RDF_TYPE_URI is not referenced elsewhere.
_ = RDF_TYPE_URI
