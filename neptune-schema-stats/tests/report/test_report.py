"""Tests for the PG report renderer."""

from __future__ import annotations

import re

import pytest

from neptune_schema_stats.correlator import correlate_pg
from neptune_schema_stats.models import PGSchema, PGSummary
from neptune_schema_stats.report import render_pg_report
from neptune_schema_stats.report._shared import AMBIGUITY_MARKER, EXACT_MARKER


@pytest.fixture
def air_routes_report(pg_summary_json, pg_schema_json):
    summary = PGSummary.from_json(pg_summary_json)
    schema = PGSchema.from_json(pg_schema_json)
    result = correlate_pg(summary, schema)
    return render_pg_report(summary, schema, result, endpoint="test-cluster", details=True)


class TestHeader:
    def test_shows_endpoint_when_provided(self, air_routes_report):
        assert "Endpoint:" in air_routes_report
        assert "test-cluster" in air_routes_report

    def test_shows_totals(self, air_routes_report):
        assert "3,748" in air_routes_report  # numNodes
        assert "51,300" in air_routes_report  # numEdges

    def test_shows_schema_state(self, air_routes_report):
        assert "Schema state:" in air_routes_report
        assert "Completed" in air_routes_report


class TestNodeLabelsSection:
    def test_lists_exact_labels(self, air_routes_report):
        assert "Node labels (exact matches)" in air_routes_report
        assert "airport" in air_routes_report
        assert "version" in air_routes_report

    def test_shows_airport_count(self, air_routes_report):
        # 3503 airports from exact matches (3471 + 32)
        assert "3,503" in air_routes_report

    def test_shows_property_fill(self, air_routes_report):
        # Airport has all 12 properties at 100% → they appear bare (no %)
        # For a spot check, 'city' should appear somewhere
        assert "city" in air_routes_report

    def test_shows_outgoing_edges_for_airport(self, air_routes_report):
        # airport's valid outgoing is {route}
        assert "route" in air_routes_report

    def test_shows_coverage_summary(self, air_routes_report):
        assert "Mapped exactly:" in air_routes_report
        assert "Ambiguous:" in air_routes_report


class TestAmbiguousSection:
    def test_section_present_for_air_routes(self, air_routes_report):
        assert AMBIGUITY_MARKER in air_routes_report
        assert "Ambiguous node groups" in air_routes_report

    def test_continent_country_bucket_appears(self, air_routes_report):
        # 161 nodes in the continent|country bucket
        assert "continent | country" in air_routes_report
        assert "161" in air_routes_report

    def test_fully_ambiguous_bucket_appears(self, air_routes_report):
        # 83 nodes matching all 4 labels
        assert "airport | continent | country | version" in air_routes_report
        assert "83" in air_routes_report


class TestEdgeLabelsSection:
    def test_shows_route_exact_count(self, air_routes_report):
        assert "route" in air_routes_report
        assert "50,532" in air_routes_report

    def test_shows_implicit_empty_edges_for_contains(self, air_routes_report):
        # 51300 - 50532 = 768 empty-property edges attributed to 'contains'
        assert "contains" in air_routes_report
        assert "768" in air_routes_report

    def test_shows_exact_marker_for_matched_edges(self, air_routes_report):
        assert EXACT_MARKER in air_routes_report

    def test_source_labels_appear_for_edge_labels(self, air_routes_report):
        # 'contains' sources per labelTriples are continent and country.
        assert "continent, country" in air_routes_report


class TestPercentages:
    def test_percentages_look_reasonable(self, air_routes_report):
        # 3503 / 3748 ≈ 93.5%
        assert re.search(r"93\.\d%", air_routes_report)


class TestMultiZeroPropertyEdgeRendering:
    """Report should surface ambiguous implicit-empty edges (identity-graph pattern)."""

    def test_ambiguous_edge_group_appears_in_edge_table(self):
        summary_json = {
            "status": "200 OK",
            "payload": {
                "version": "v1",
                "lastStatisticsComputationTime": "2026-08-09T00:00:00.000Z",
                "graphSummary": {
                    "numNodes": 10,
                    "numEdges": 50,
                    "numNodeLabels": 1,
                    "numEdgeLabels": 2,
                    "nodeLabels": ["A"],
                    "edgeLabels": ["links", "uses"],
                    "numNodeProperties": 1,
                    "numEdgeProperties": 0,
                    "nodeProperties": [{"id": 10}],
                    "edgeProperties": [],
                    "totalNodePropertyValues": 10,
                    "totalEdgePropertyValues": 0,
                    "nodeStructures": [
                        {
                            "count": 10,
                            "nodeProperties": ["id"],
                            "distinctOutgoingEdgeLabels": ["links", "uses"],
                        },
                    ],
                    "edgeStructures": [],
                },
            },
        }
        schema_json = {
            "results": [
                {
                    "schema": {
                        "status": {
                            "state": "Completed",
                            "concurrency": "16",
                            "lastComputedTimestamp": "2026-08-09T00:00:00Z",
                            "progressPercentage": "100",
                        },
                        "nodeLabels": ["A"],
                        "edgeLabels": ["links", "uses"],
                        "nodeLabelDetails": {"A": {"properties": {"id": ["String"]}}},
                        "edgeLabelDetails": {
                            "links": {"properties": {}},
                            "uses": {"properties": {}},
                        },
                        "labelTriples": [
                            {"~type": "links", "~from": "A", "~to": "A"},
                            {"~type": "uses", "~from": "A", "~to": "A"},
                        ],
                    }
                }
            ]
        }
        summary = PGSummary.from_json(summary_json)
        schema = PGSchema.from_json(schema_json)
        result = correlate_pg(summary, schema)
        report = render_pg_report(summary, schema, result, details=True)

        # Both labels should appear as separate rows with range bounds (not just
        # 'links | uses' as before). Each min is 10 (all 10 nodes emit both).
        # Max for each is 50 - other's min = 40.
        assert "links" in report
        assert "uses" in report
        assert "40" in report  # max = 50 - 10 = 40
        assert AMBIGUITY_MARKER in report
        # The "range" explanatory footer should appear.
        assert "Range bounds are derived" in report


class TestMultiLabelCombinationsTable:
    """Multi-labelset rows should render in a separate 'Multi-label combinations'
    table so they don't visually collide with literal labels containing colons."""

    def test_split_table_when_partition_populated(self):
        from neptune_schema_stats.correlator import (
            LabelSetCount,
            LabelStats,
            NodeLabelBound,
            PGCorrelationResult,
        )
        from neptune_schema_stats.report import render_pg_report

        # Include BOTH a literal label named "A:B" AND a multi-label combo
        # of :A + :B, to prove the two are visually distinguished.
        result = PGCorrelationResult(
            node_matches=(),
            edge_matches=(),
            label_stats={
                "A": LabelStats(
                    label="A", node_count=100, property_fill_counts={}, contributing_structures=1
                ),
                "B": LabelStats(
                    label="B", node_count=50, property_fill_counts={}, contributing_structures=1
                ),
                "A:B": LabelStats(
                    label="A:B", node_count=1, property_fill_counts={}, contributing_structures=1
                ),
            },
            edge_stats={},
            ambiguous_node_groups=(),
            unmapped_node_structures=(),
            implicit_empty_edges=None,
            node_label_bounds={
                "A": NodeLabelBound(label="A", min_count=100, max_count=100, exact_structures=1),
                "B": NodeLabelBound(label="B", min_count=50, max_count=50, exact_structures=1),
                "A:B": NodeLabelBound(label="A:B", min_count=1, max_count=1, exact_structures=1),
            },
            edge_label_bounds={},
            label_partition=(
                LabelSetCount(labels=("A",), count=97),
                LabelSetCount(labels=("B",), count=47),
                LabelSetCount(labels=("A", "B"), count=3),
            ),
        )
        summary = PGSummary.from_json(
            {
                "payload": {
                    "graphSummary": {
                        "numNodes": 151,
                        "numEdges": 0,
                        "numNodeLabels": 3,
                        "numEdgeLabels": 0,
                        "nodeLabels": ["A", "B", "A:B"],
                        "edgeLabels": [],
                        "nodeStructures": [],
                        "edgeStructures": [],
                    }
                }
            }
        )
        schema = PGSchema.from_json(
            {
                "results": [
                    {
                        "schema": {
                            "status": {
                                "state": "Completed",
                                "concurrency": "16",
                                "lastComputedTimestamp": "",
                                "progressPercentage": "100",
                                "errorMessage": "",
                            },
                            "nodeLabels": ["A", "B", "A:B"],
                            "edgeLabels": [],
                            "nodeLabelDetails": {},
                            "edgeLabelDetails": {},
                            "labelTriples": [],
                        }
                    }
                ]
            }
        )
        output = render_pg_report(summary, schema, result, endpoint="test")
        # Two separate table headers should appear
        assert "Node labels" in output
        assert "Multi-label combinations" in output
        # The literal "A:B" label row is in Node labels with count 1
        assert re.search(r"\bA:B\b\s+1\b", output)
        # And the multi-combo row is in the other section with count 3
        assert re.search(r"A:B\s+3\b", output)
