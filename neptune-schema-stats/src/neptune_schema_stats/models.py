"""Typed data models for Neptune API responses.

These are plain dataclasses that mirror the shape of the Neptune Graph Summary
and PG Schema API responses. They are parsed once from raw JSON at the client
boundary, then consumed as strongly-typed objects throughout the rest of the tool.

Design principles:
- Frozen dataclasses to prevent accidental mutation after parsing.
- No business logic here — parsing only. Correlation lives in a separate module.
- Preserve the API's own field names where reasonable to make debugging easier.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

# Well-known RDF URI used to detect typed subjects.
RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


class GraphMode(StrEnum):
    """The graph model of a Neptune cluster."""

    PG = "pg"
    RDF = "rdf"


class SchemaState(StrEnum):
    """Lifecycle state for the async PG schema computation."""

    NOT_STARTED = "NotStarted"
    IN_PROGRESS = "InProgress"
    COMPLETED = "Completed"
    STOPPED = "Stopped"
    FAILED = "Failed"


# ---------------------------------------------------------------------------
# PG Summary API
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PGNodeStructure:
    """A property-graph node characteristic set.

    Represents ``count`` distinct nodes that share the same property signature
    and outgoing edge-label set. Nodes are not labeled here — the label is
    inferred later via correlation with the Schema API.
    """

    count: int
    node_properties: tuple[str, ...]
    distinct_outgoing_edge_labels: tuple[str, ...]

    @classmethod
    def from_json(cls, obj: dict[str, Any]) -> PGNodeStructure:
        return cls(
            count=int(obj["count"]),
            node_properties=tuple(obj.get("nodeProperties", [])),
            distinct_outgoing_edge_labels=tuple(obj.get("distinctOutgoingEdgeLabels", [])),
        )


@dataclass(frozen=True, slots=True)
class PGEdgeStructure:
    """A property-graph edge characteristic set.

    Represents ``count`` distinct edges that share the same property signature.
    The edge label is inferred later via correlation with the Schema API.
    """

    count: int
    edge_properties: tuple[str, ...]

    @classmethod
    def from_json(cls, obj: dict[str, Any]) -> PGEdgeStructure:
        return cls(
            count=int(obj["count"]),
            edge_properties=tuple(obj.get("edgeProperties", [])),
        )


@dataclass(frozen=True, slots=True)
class PGSummary:
    """Full parsed PG summary response (mode=detailed)."""

    num_nodes: int
    num_edges: int
    num_node_labels: int
    num_edge_labels: int
    node_labels: tuple[str, ...]
    edge_labels: tuple[str, ...]
    num_node_properties: int
    num_edge_properties: int
    # Property → count of nodes with that property (graph-wide).
    node_properties: dict[str, int]
    # Property → count of edges with that property (graph-wide).
    edge_properties: dict[str, int]
    total_node_property_values: int
    total_edge_property_values: int
    node_structures: tuple[PGNodeStructure, ...]
    edge_structures: tuple[PGEdgeStructure, ...]
    last_statistics_computation_time: str

    @classmethod
    def from_json(cls, envelope: dict[str, Any]) -> PGSummary:
        """Parse from the full API response envelope: ``{status, payload: {...}}``."""
        payload = envelope["payload"]
        graph = payload["graphSummary"]

        def _flatten_kv_list(items: list[dict[str, int]] | None) -> dict[str, int]:
            """Convert ``[{"code": 3748}, ...]`` into a single dict."""
            result: dict[str, int] = {}
            for item in items or []:
                for key, value in item.items():
                    result[key] = int(value)
            return result

        return cls(
            num_nodes=int(graph["numNodes"]),
            num_edges=int(graph["numEdges"]),
            num_node_labels=int(graph.get("numNodeLabels", 0)),
            num_edge_labels=int(graph.get("numEdgeLabels", 0)),
            node_labels=tuple(graph.get("nodeLabels", [])),
            edge_labels=tuple(graph.get("edgeLabels", [])),
            num_node_properties=int(graph.get("numNodeProperties", 0)),
            num_edge_properties=int(graph.get("numEdgeProperties", 0)),
            node_properties=_flatten_kv_list(graph.get("nodeProperties")),
            edge_properties=_flatten_kv_list(graph.get("edgeProperties")),
            total_node_property_values=int(graph.get("totalNodePropertyValues", 0)),
            total_edge_property_values=int(graph.get("totalEdgePropertyValues", 0)),
            node_structures=tuple(
                PGNodeStructure.from_json(ns) for ns in graph.get("nodeStructures", [])
            ),
            edge_structures=tuple(
                PGEdgeStructure.from_json(es) for es in graph.get("edgeStructures", [])
            ),
            last_statistics_computation_time=_iso_or_str(
                payload.get("lastStatisticsComputationTime", "")
            ),
        )


# ---------------------------------------------------------------------------
# PG Schema API
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LabelTriple:
    """A ``{~from, ~type, ~to}`` connection pattern from ``pg_schema()``."""

    from_label: str
    edge_type: str
    to_label: str

    @classmethod
    def from_json(cls, obj: dict[str, Any]) -> LabelTriple:
        return cls(
            from_label=obj["~from"],
            edge_type=obj["~type"],
            to_label=obj["~to"],
        )


@dataclass(frozen=True, slots=True)
class PGSchemaStatus:
    """Lifecycle status of the PG schema computation."""

    state: SchemaState
    concurrency: str
    last_computed_timestamp: str
    progress_percentage: str
    error_message: str | None = None

    @classmethod
    def from_json(cls, obj: dict[str, Any]) -> PGSchemaStatus:
        return cls(
            state=SchemaState(obj.get("state", "NotStarted")),
            concurrency=str(obj.get("concurrency", "0")),
            last_computed_timestamp=obj.get("lastComputedTimestamp", ""),
            progress_percentage=str(obj.get("progressPercentage", "0")),
            error_message=obj.get("errorMessage"),
        )


@dataclass(frozen=True, slots=True)
class PGSchema:
    """Full parsed PG schema response from ``CALL neptune.graph.pg_schema()``."""

    status: PGSchemaStatus
    node_labels: tuple[str, ...]
    edge_labels: tuple[str, ...]
    # Label → property name → tuple of observed datatypes.
    node_label_details: dict[str, dict[str, tuple[str, ...]]]
    edge_label_details: dict[str, dict[str, tuple[str, ...]]]
    label_triples: tuple[LabelTriple, ...]

    @classmethod
    def from_json(cls, envelope: dict[str, Any]) -> PGSchema:
        """Parse from the openCypher result envelope: ``{results: [{schema: {...}}]}``."""
        results = envelope.get("results", [])
        if not results:
            raise ValueError("pg_schema() returned no results")
        schema = results[0].get("schema")
        if schema is None:
            raise ValueError("pg_schema() result missing 'schema' key")

        def _parse_label_details(
            raw: dict[str, Any] | None,
        ) -> dict[str, dict[str, tuple[str, ...]]]:
            out: dict[str, dict[str, tuple[str, ...]]] = {}
            for label, entry in (raw or {}).items():
                properties: dict[str, tuple[str, ...]] = {}
                for prop_name, datatypes in entry.get("properties", {}).items():
                    if isinstance(datatypes, list):
                        properties[prop_name] = tuple(str(dt) for dt in datatypes)
                    else:
                        properties[prop_name] = (str(datatypes),)
                out[label] = properties
            return out

        return cls(
            status=PGSchemaStatus.from_json(schema.get("status", {})),
            node_labels=tuple(schema.get("nodeLabels", [])),
            edge_labels=tuple(schema.get("edgeLabels", [])),
            node_label_details=_parse_label_details(schema.get("nodeLabelDetails")),
            edge_label_details=_parse_label_details(schema.get("edgeLabelDetails")),
            label_triples=tuple(LabelTriple.from_json(t) for t in schema.get("labelTriples", [])),
        )

    def state(self) -> SchemaState:
        """Convenience accessor for the schema computation state."""
        return self.status.state

    def is_usable(self) -> bool:
        """Whether the schema has any content — Completed or partial InProgress."""
        return self.status.state in (SchemaState.COMPLETED, SchemaState.IN_PROGRESS) and bool(
            self.node_label_details
        )


# ---------------------------------------------------------------------------
# RDF Summary API
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RDFSubjectStructure:
    """An RDF subject characteristic set.

    Represents ``count`` distinct subjects that share the same predicate set.
    The predicate list may include ``rdf:type`` (indicating typed subjects) or
    may not (indicating blank nodes, reified statements, or list elements).
    """

    count: int
    predicates: tuple[str, ...]

    @classmethod
    def from_json(cls, obj: dict[str, Any]) -> RDFSubjectStructure:
        return cls(
            count=int(obj["count"]),
            predicates=tuple(obj.get("predicates", [])),
        )

    def has_rdf_type(self) -> bool:
        """Whether this structure includes the ``rdf:type`` predicate."""
        return RDF_TYPE_URI in self.predicates


@dataclass(frozen=True, slots=True)
class RDFSummary:
    """Full parsed RDF summary response (mode=detailed)."""

    num_distinct_subjects: int
    num_distinct_predicates: int
    num_quads: int
    num_classes: int
    classes: tuple[str, ...]
    # Predicate URI → occurrence count (already precise from the API).
    predicates: dict[str, int]
    subject_structures: tuple[RDFSubjectStructure, ...]
    last_statistics_computation_time: str

    @classmethod
    def from_json(cls, envelope: dict[str, Any]) -> RDFSummary:
        """Parse from the full API response envelope: ``{status, payload: {...}}``."""
        payload = envelope["payload"]
        graph = payload["graphSummary"]

        predicates: dict[str, int] = {}
        for item in graph.get("predicates", []):
            for uri, count in item.items():
                predicates[uri] = int(count)

        return cls(
            num_distinct_subjects=int(graph["numDistinctSubjects"]),
            num_distinct_predicates=int(graph.get("numDistinctPredicates", 0)),
            num_quads=int(graph["numQuads"]),
            num_classes=int(graph.get("numClasses", 0)),
            classes=tuple(graph.get("classes", [])),
            predicates=predicates,
            subject_structures=tuple(
                RDFSubjectStructure.from_json(ss) for ss in graph.get("subjectStructures", [])
            ),
            last_statistics_computation_time=_iso_or_str(
                payload.get("lastStatisticsComputationTime", "")
            ),
        )


# ---------------------------------------------------------------------------
# Error envelope
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NeptuneAPIError:
    """Parsed Neptune API error response body."""

    code: str
    detailed_message: str
    request_id: str = ""
    http_status: int = 0
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, body: dict[str, Any], http_status: int = 0) -> NeptuneAPIError:
        return cls(
            code=str(body.get("code", "Unknown")),
            detailed_message=str(body.get("detailedMessage", "")),
            request_id=str(body.get("requestId", "")),
            http_status=http_status,
            raw=body,
        )

    def __str__(self) -> str:
        return f"[{self.code}] {self.detailed_message}"


def _iso_or_str(value: Any) -> str:
    """Coerce a value to a string. Uses ``isoformat()`` for datetime-like
    values so we get stable ISO-8601 rather than the ``str(datetime)`` form."""
    if value is None or value == "":
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
