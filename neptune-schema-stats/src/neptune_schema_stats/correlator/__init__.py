"""Neptune graph correlator — reconciles Graph Summary and Schema data.

Public API:

- ``correlate_pg`` — property-graph correlator
- ``correlate_rdf`` — RDF correlator
- All :mod:`~neptune_schema_stats.correlator.types` dataclasses

Also re-exports ``validate_scan_totals`` for post-scan verification.
"""

from neptune_schema_stats.correlator.pg import (
    build_label_index,
    correlate_pg,
    match_edge_structure,
    match_structure_to_labels,
)
from neptune_schema_stats.correlator.rdf import (
    ClassCount,
    PredicateCategory,
    PredicateStats,
    RDFCorrelationResult,
    SubjectStructureAnalysis,
    classify_predicate,
    correlate_rdf,
    result_to_jsonable,
)
from neptune_schema_stats.correlator.types import (
    AmbiguousNodeGroup,
    ConsistencyWarning,
    EdgeLabelBound,
    EdgeMatch,
    EdgesByTargetLabel,
    EdgeStats,
    ImplicitEmptyEdges,
    LabelSetCount,
    LabelSignature,
    LabelStats,
    NodeLabelBound,
    NodeMatch,
    PGCorrelationResult,
)
from neptune_schema_stats.correlator.validate import validate_scan_totals

__all__ = [
    "AmbiguousNodeGroup",
    "ClassCount",
    "ConsistencyWarning",
    "EdgeLabelBound",
    "EdgeMatch",
    "EdgeStats",
    "EdgesByTargetLabel",
    "ImplicitEmptyEdges",
    "LabelSetCount",
    "LabelSignature",
    "LabelStats",
    "NodeLabelBound",
    "NodeMatch",
    "PGCorrelationResult",
    "PredicateCategory",
    "PredicateStats",
    "RDFCorrelationResult",
    "SubjectStructureAnalysis",
    "build_label_index",
    "classify_predicate",
    "correlate_pg",
    "correlate_rdf",
    "match_edge_structure",
    "match_structure_to_labels",
    "result_to_jsonable",
    "validate_scan_totals",
]
