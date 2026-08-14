"""Tests for the data model parsers in neptune_schema_stats.models."""

from __future__ import annotations

from neptune_schema_stats.models import (
    RDF_TYPE_URI,
    NeptuneAPIError,
    PGSchema,
    PGSummary,
    RDFSummary,
    SchemaState,
)


class TestPGSummary:
    def test_parses_totals(self, pg_summary_json):
        summary = PGSummary.from_json(pg_summary_json)
        assert summary.num_nodes == 3748
        assert summary.num_edges == 51300
        assert summary.num_node_labels == 4
        assert summary.num_edge_labels == 2
        assert summary.last_statistics_computation_time == "2023-03-01T14:35:03.804Z"

    def test_parses_label_lists(self, pg_summary_json):
        summary = PGSummary.from_json(pg_summary_json)
        assert set(summary.node_labels) == {"continent", "country", "version", "airport"}
        assert set(summary.edge_labels) == {"contains", "route"}

    def test_flattens_property_counts(self, pg_summary_json):
        summary = PGSummary.from_json(pg_summary_json)
        assert summary.node_properties["code"] == 3748
        assert summary.node_properties["city"] == 3503
        assert summary.node_properties["author"] == 1
        assert summary.edge_properties["dist"] == 50532

    def test_parses_node_structures(self, pg_summary_json):
        summary = PGSummary.from_json(pg_summary_json)
        assert len(summary.node_structures) == 5

        # Total node count from structures should match num_nodes.
        total = sum(s.count for s in summary.node_structures)
        assert total == summary.num_nodes

    def test_node_structure_has_outgoing_edges(self, pg_summary_json):
        summary = PGSummary.from_json(pg_summary_json)
        # The 3471-count structure has 'route' as outgoing.
        s = next(s for s in summary.node_structures if s.count == 3471)
        assert s.distinct_outgoing_edge_labels == ("route",)
        assert "city" in s.node_properties
        assert "code" in s.node_properties

    def test_node_structure_can_have_no_outgoing_edges(self, pg_summary_json):
        summary = PGSummary.from_json(pg_summary_json)
        # The 32-count structure has full airport props but no outgoing edges.
        s = next(s for s in summary.node_structures if s.count == 32)
        assert s.distinct_outgoing_edge_labels == ()
        assert len(s.node_properties) == 12

    def test_parses_edge_structures(self, pg_summary_json):
        summary = PGSummary.from_json(pg_summary_json)
        assert len(summary.edge_structures) == 1
        es = summary.edge_structures[0]
        assert es.count == 50532
        assert es.edge_properties == ("dist",)


class TestPGSchema:
    def test_parses_status_completed(self, pg_schema_json):
        schema = PGSchema.from_json(pg_schema_json)
        assert schema.state() == SchemaState.COMPLETED
        assert schema.status.progress_percentage == "100"
        assert schema.is_usable() is True

    def test_parses_labels(self, pg_schema_json):
        schema = PGSchema.from_json(pg_schema_json)
        assert set(schema.node_labels) == {"version", "continent", "airport", "country"}
        assert set(schema.edge_labels) == {"route", "contains"}

    def test_parses_node_label_details(self, pg_schema_json):
        schema = PGSchema.from_json(pg_schema_json)
        airport = schema.node_label_details["airport"]
        assert "city" in airport
        assert airport["city"] == ("String",)
        assert airport["lat"] == ("Double",)
        assert len(airport) == 12  # air-routes airport has 12 properties

    def test_parses_edge_label_details(self, pg_schema_json):
        schema = PGSchema.from_json(pg_schema_json)
        assert schema.edge_label_details["route"] == {"dist": ("Int",)}
        assert schema.edge_label_details["contains"] == {}

    def test_parses_label_triples(self, pg_schema_json):
        schema = PGSchema.from_json(pg_schema_json)
        assert len(schema.label_triples) == 3
        route_triple = next(t for t in schema.label_triples if t.edge_type == "route")
        assert route_triple.from_label == "airport"
        assert route_triple.to_label == "airport"

        contains_sources = {t.from_label for t in schema.label_triples if t.edge_type == "contains"}
        assert contains_sources == {"country", "continent"}

    def test_not_started_state_is_not_usable(self, pg_schema_not_started_json):
        schema = PGSchema.from_json(pg_schema_not_started_json)
        assert schema.state() == SchemaState.NOT_STARTED
        assert schema.is_usable() is False
        assert schema.node_label_details == {}


class TestRDFSummary:
    def test_parses_totals(self, rdf_summary_json):
        summary = RDFSummary.from_json(rdf_summary_json)
        assert summary.num_distinct_subjects == 54403
        assert summary.num_quads == 158571
        assert summary.num_classes == 4
        assert summary.num_distinct_predicates == 19

    def test_parses_classes(self, rdf_summary_json):
        summary = RDFSummary.from_json(rdf_summary_json)
        assert len(summary.classes) == 4
        assert any("Airport" in c for c in summary.classes)

    def test_predicate_counts_include_rdf_type(self, rdf_summary_json):
        summary = RDFSummary.from_json(rdf_summary_json)
        assert summary.predicates[RDF_TYPE_URI] == 3747

    def test_parses_subject_structures(self, rdf_summary_json):
        summary = RDFSummary.from_json(rdf_summary_json)
        assert len(summary.subject_structures) == 6

        counts = sorted((s.count for s in summary.subject_structures), reverse=True)
        assert counts == [50656, 3471, 238, 31, 6, 1]

    def test_rdf_type_detection(self, rdf_summary_json):
        summary = RDFSummary.from_json(rdf_summary_json)
        # The 50656-subject structure has ONLY dist — no rdf:type.
        stmt_structure = next(s for s in summary.subject_structures if s.count == 50656)
        assert stmt_structure.has_rdf_type() is False

        # The 3471-airport structure includes rdf:type.
        airport_structure = next(s for s in summary.subject_structures if s.count == 3471)
        assert airport_structure.has_rdf_type() is True

    def test_typed_vs_untyped_partition(self, rdf_summary_json):
        summary = RDFSummary.from_json(rdf_summary_json)
        typed = [s for s in summary.subject_structures if s.has_rdf_type()]
        untyped = [s for s in summary.subject_structures if not s.has_rdf_type()]

        typed_count = sum(s.count for s in typed)
        untyped_count = sum(s.count for s in untyped)

        # 3471 + 238 + 31 + 6 + 1 = 3747 typed subjects — matches rdf:type predicate count.
        assert typed_count == 3747
        # 50656 statement-level subjects with just 'dist'.
        assert untyped_count == 50656


class TestNeptuneAPIError:
    def test_parses_error_envelope(self, stats_not_available_json):
        err = NeptuneAPIError.from_json(stats_not_available_json, http_status=400)
        assert err.code == "StatisticsNotAvailableException"
        assert "Statistics are not available" in err.detailed_message
        assert err.http_status == 400
        assert err.request_id
