"""Tests for the PG correlator.

Uses the air-routes docs samples plus synthetic edge cases. Air-routes is a
particularly good real-world test because it has both an exact-match case
(``airport``, ``version``) and a genuine ambiguity (``continent`` / ``country``
share the same property set ``{code, desc, type}``).
"""

from __future__ import annotations

import pytest

from neptune_schema_stats.correlator import (
    LabelSignature,
    build_label_index,
    correlate_pg,
    match_edge_structure,
    match_structure_to_labels,
)
from neptune_schema_stats.models import (
    PGEdgeStructure,
    PGNodeStructure,
    PGSchema,
    PGSummary,
)

# ---------------------------------------------------------------------------
# build_label_index
# ---------------------------------------------------------------------------


class TestBuildLabelIndex:
    def test_indexes_all_labels(self, pg_schema_json):
        schema = PGSchema.from_json(pg_schema_json)
        index = build_label_index(schema)
        assert set(index.keys()) == {"airport", "continent", "country", "version"}

    def test_property_sets_match_schema(self, pg_schema_json):
        schema = PGSchema.from_json(pg_schema_json)
        index = build_label_index(schema)
        assert index["airport"].properties == frozenset(
            {
                "city",
                "code",
                "country",
                "desc",
                "elev",
                "icao",
                "lat",
                "lon",
                "longest",
                "region",
                "runways",
                "type",
            }
        )
        assert index["continent"].properties == frozenset({"code", "desc", "type"})
        assert index["country"].properties == frozenset({"code", "desc", "type"})
        assert index["version"].properties == frozenset({"author", "code", "date", "desc", "type"})

    def test_valid_outgoing_derived_from_label_triples(self, pg_schema_json):
        schema = PGSchema.from_json(pg_schema_json)
        index = build_label_index(schema)
        assert index["airport"].valid_outgoing == frozenset({"route"})
        assert index["continent"].valid_outgoing == frozenset({"contains"})
        assert index["country"].valid_outgoing == frozenset({"contains"})
        assert index["version"].valid_outgoing == frozenset()

    def test_valid_incoming_derived_from_label_triples(self, pg_schema_json):
        schema = PGSchema.from_json(pg_schema_json)
        index = build_label_index(schema)
        # Airport is the target of both route and contains.
        assert index["airport"].valid_incoming == frozenset({"route", "contains"})
        assert index["continent"].valid_incoming == frozenset()

    def test_empty_index_when_schema_not_usable(self, pg_schema_not_started_json):
        schema = PGSchema.from_json(pg_schema_not_started_json)
        assert build_label_index(schema) == {}


# ---------------------------------------------------------------------------
# match_structure_to_labels — the heart of the algorithm
# ---------------------------------------------------------------------------


class TestMatchStructure:
    @pytest.fixture
    def air_routes_index(self, pg_schema_json) -> dict[str, LabelSignature]:
        schema = PGSchema.from_json(pg_schema_json)
        return build_label_index(schema)

    def test_full_airport_structure_matches_airport_exactly(self, air_routes_index):
        # 3471 airports with all 12 properties + outgoing route
        struct = PGNodeStructure(
            count=3471,
            node_properties=(
                "city",
                "code",
                "country",
                "desc",
                "elev",
                "icao",
                "lat",
                "lon",
                "longest",
                "region",
                "runways",
                "type",
            ),
            distinct_outgoing_edge_labels=("route",),
        )
        assert match_structure_to_labels(struct, air_routes_index) == ("airport",)

    def test_airport_without_routes_still_matches_airport(self, air_routes_index):
        # 32 airports with full properties but no outgoing edges — still airport.
        struct = PGNodeStructure(
            count=32,
            node_properties=(
                "city",
                "code",
                "country",
                "desc",
                "elev",
                "icao",
                "lat",
                "lon",
                "longest",
                "region",
                "runways",
                "type",
            ),
            distinct_outgoing_edge_labels=(),
        )
        assert match_structure_to_labels(struct, air_routes_index) == ("airport",)

    def test_continent_country_signature_with_contains_is_ambiguous(self, air_routes_index):
        # 161 nodes with {code, desc, type} + outgoing contains.
        # Both continent AND country are valid — airport ruled out by 'contains',
        # version ruled out because version.valid_outgoing is empty.
        struct = PGNodeStructure(
            count=161,
            node_properties=("code", "desc", "type"),
            distinct_outgoing_edge_labels=("contains",),
        )
        assert match_structure_to_labels(struct, air_routes_index) == (
            "continent",
            "country",
        )

    def test_bare_signature_no_outgoing_is_fully_ambiguous(self, air_routes_index):
        # 83 nodes with {code, desc, type} and no outgoing edges.
        # Property subset test admits all 4 labels; edge test doesn't filter any.
        struct = PGNodeStructure(
            count=83,
            node_properties=("code", "desc", "type"),
            distinct_outgoing_edge_labels=(),
        )
        assert match_structure_to_labels(struct, air_routes_index) == (
            "airport",
            "continent",
            "country",
            "version",
        )

    def test_version_signature_matches_only_version(self, air_routes_index):
        # 1 version node with {author, code, date, desc, type} — 'author' and
        # 'date' are unique to version.
        struct = PGNodeStructure(
            count=1,
            node_properties=("author", "code", "date", "desc", "type"),
            distinct_outgoing_edge_labels=(),
        )
        assert match_structure_to_labels(struct, air_routes_index) == ("version",)

    def test_unmapped_structure_when_no_label_matches(self, air_routes_index):
        # A property not in any schema signature → 0 candidates.
        struct = PGNodeStructure(
            count=99,
            node_properties=("nonexistent_property",),
            distinct_outgoing_edge_labels=(),
        )
        assert match_structure_to_labels(struct, air_routes_index) == ()

    def test_outgoing_edge_ruling_out_all_candidates(self, air_routes_index):
        # A structure with an edge type not sourced by any candidate label.
        struct = PGNodeStructure(
            count=99,
            node_properties=("code", "desc", "type"),
            # 'route' can only come from airport, and airport has more props;
            # so property signature admits airport, then route confirms.
            # But if we swap airport-only props off, and add an impossible edge:
            distinct_outgoing_edge_labels=("nonexistent_edge",),
        )
        assert match_structure_to_labels(struct, air_routes_index) == ()


# ---------------------------------------------------------------------------
# match_edge_structure
# ---------------------------------------------------------------------------


class TestMatchEdgeStructure:
    def test_dist_signature_matches_route_only(self, pg_schema_json):
        schema = PGSchema.from_json(pg_schema_json)
        struct = PGEdgeStructure(count=50532, edge_properties=("dist",))
        assert match_edge_structure(struct, schema) == ("route",)

    def test_empty_signature_matches_contains_only_for_air_routes(self, pg_schema_json):
        # Air-routes only has one zero-property edge label ('contains').
        schema = PGSchema.from_json(pg_schema_json)
        struct = PGEdgeStructure(count=768, edge_properties=())
        assert match_edge_structure(struct, schema) == ("contains",)


# ---------------------------------------------------------------------------
# correlate_pg — end-to-end against air-routes
# ---------------------------------------------------------------------------


class TestCorrelatePGAirRoutes:
    @pytest.fixture
    def result(self, pg_summary_json, pg_schema_json):
        summary = PGSummary.from_json(pg_summary_json)
        schema = PGSchema.from_json(pg_schema_json)
        return correlate_pg(summary, schema)

    def test_exact_airport_count(self, result):
        # 3471 + 32 = 3503 airports from exact matches
        assert result.label_stats["airport"].node_count == 3503
        assert result.label_stats["airport"].contributing_structures == 2

    def test_exact_version_count(self, result):
        assert result.label_stats["version"].node_count == 1
        assert result.label_stats["version"].contributing_structures == 1

    def test_continent_and_country_have_zero_exact_count(self, result):
        # No structure was uniquely a continent or country — both are ambiguous.
        assert result.label_stats["continent"].node_count == 0
        assert result.label_stats["country"].node_count == 0

    def test_ambiguous_groups_reported(self, result):
        groups = result.ambiguous_node_groups
        # Two ambiguity buckets:
        #   1. {continent, country} for 161 nodes (with contains outgoing)
        #   2. {airport, continent, country, version} for 83 nodes (no outgoing)
        assert len(groups) == 2

        # Groups are sorted by total_count desc.
        assert groups[0].candidate_labels == ("continent", "country")
        assert groups[0].total_count == 161

        assert groups[1].candidate_labels == ("airport", "continent", "country", "version")
        assert groups[1].total_count == 83

    def test_totals_sum_to_num_nodes(self, result, pg_summary_json):
        summary = PGSummary.from_json(pg_summary_json)
        total = (
            result.total_exact_nodes + result.total_ambiguous_nodes + result.total_unmapped_nodes
        )
        assert total == summary.num_nodes == 3748

    def test_no_unmapped_structures_for_air_routes(self, result):
        assert result.unmapped_node_structures == ()

    def test_property_fill_counts_for_airport(self, result):
        # All 3503 airports have every airport property populated (both
        # contributing structures share the full 12-property signature).
        fill = result.label_stats["airport"].property_fill_counts
        for prop in (
            "city",
            "code",
            "country",
            "desc",
            "elev",
            "icao",
            "lat",
            "lon",
            "longest",
            "region",
            "runways",
            "type",
        ):
            assert fill[prop] == 3503, f"{prop} should be 3503, got {fill[prop]}"

    # ---- edges ----------------------------------------------------------------

    def test_route_edge_matches_exactly(self, result):
        assert result.edge_stats["route"].edge_count == 50532
        assert result.edge_stats["route"].property_signature == ("dist",)

    def test_implicit_empty_edges_attributed_to_contains(self, result):
        # num_edges (51300) - explicit edges (50532) = 768 implicit-empty edges,
        # attributed to 'contains' since it's the only zero-property edge label.
        assert result.implicit_empty_edges is not None
        assert result.implicit_empty_edges.count == 768
        assert result.implicit_empty_edges.candidate_labels == ("contains",)
        assert result.implicit_empty_edges.is_exact

    def test_edge_label_bounds_route_is_exact(self, result):
        b = result.edge_label_bounds["route"]
        assert b.is_exact
        assert b.min_count == 50532
        assert b.max_count == 50532
        assert b.source_labels == ("airport",)
        assert b.property_signature == ("dist",)

    def test_edge_label_bounds_contains_is_exact_by_elimination(self, result):
        # Only one zero-property edge label — contains absorbs the implicit residual.
        b = result.edge_label_bounds["contains"]
        assert b.is_exact
        assert b.min_count == 768
        assert b.max_count == 768
        # Sources per labelTriples: country and continent both emit contains.
        assert b.source_labels == ("continent", "country")
        assert b.property_signature == ()


# ---------------------------------------------------------------------------
# Multi-zero-property-edge synthetic case (identity-graph pattern)
# ---------------------------------------------------------------------------


class TestMultiZeroPropertyEdges:
    """Replicates the identity-graph pattern: multiple zero-property edge labels
    sharing an implicit-empty residual, which must be reported as ambiguous."""

    def test_ambiguous_implicit_empty_when_multiple_zero_prop_labels(self):
        # Two-edge-label graph where both edges have empty property signatures.
        summary_json = {
            "status": "200 OK",
            "payload": {
                "version": "v1",
                "lastStatisticsComputationTime": "2026-08-09T00:00:00.000Z",
                "graphSummary": {
                    "numNodes": 100,
                    "numEdges": 500,
                    "numNodeLabels": 2,
                    "numEdgeLabels": 2,
                    "nodeLabels": ["A", "B"],
                    "edgeLabels": ["links", "uses"],
                    "numNodeProperties": 1,
                    "numEdgeProperties": 0,
                    "nodeProperties": [{"id": 100}],
                    "edgeProperties": [],
                    "totalNodePropertyValues": 100,
                    "totalEdgePropertyValues": 0,
                    "nodeStructures": [
                        {
                            "count": 60,
                            "nodeProperties": ["id"],
                            "distinctOutgoingEdgeLabels": ["links"],
                        },
                        {
                            "count": 40,
                            "nodeProperties": ["id"],
                            "distinctOutgoingEdgeLabels": ["uses"],
                        },
                    ],
                    "edgeStructures": [],  # no explicit edge structures
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
                        "nodeLabels": ["A", "B"],
                        "edgeLabels": ["links", "uses"],
                        "nodeLabelDetails": {
                            "A": {"properties": {"id": ["String"]}},
                            "B": {"properties": {"id": ["String"]}},
                        },
                        "edgeLabelDetails": {
                            "links": {"properties": {}},
                            "uses": {"properties": {}},
                        },
                        "labelTriples": [
                            {"~type": "links", "~from": "A", "~to": "B"},
                            {"~type": "uses", "~from": "B", "~to": "A"},
                        ],
                    }
                }
            ]
        }
        summary = PGSummary.from_json(summary_json)
        schema = PGSchema.from_json(schema_json)
        result = correlate_pg(summary, schema)

        # Every explicit edge structure is empty, so all 500 edges are implicit-empty
        # and split ambiguously between 'links' and 'uses'.
        assert result.edge_stats == {}
        assert result.implicit_empty_edges is not None
        assert result.implicit_empty_edges.count == 500
        assert result.implicit_empty_edges.candidate_labels == ("links", "uses")
        assert result.implicit_empty_edges.is_ambiguous


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------


class TestEdgeLabelBoundsIdentityPattern:
    """Verifies min/max edge-label bounds against a small synthetic dataset
    that mirrors the identity-graph shape (multiple zero-property edge labels
    with different node-structure floors)."""

    @pytest.fixture
    def result(self):
        # 3 node structures, each emitting a distinct zero-property edge type;
        # plus a residual implicit-empty pool that must be split by minimums.
        summary_json = {
            "status": "200 OK",
            "payload": {
                "version": "v1",
                "lastStatisticsComputationTime": "2026-08-09T00:00:00.000Z",
                "graphSummary": {
                    "numNodes": 3000,
                    "numEdges": 10000,
                    "numNodeLabels": 3,
                    "numEdgeLabels": 3,
                    "nodeLabels": ["A", "B", "C"],
                    "edgeLabels": ["e1", "e2", "e3"],
                    "numNodeProperties": 1,
                    "numEdgeProperties": 0,
                    "nodeProperties": [{"id": 3000}],
                    "edgeProperties": [],
                    "totalNodePropertyValues": 3000,
                    "totalEdgePropertyValues": 0,
                    "nodeStructures": [
                        # 100 A-nodes each emit >=1 e1  → min(e1) >= 100
                        {
                            "count": 100,
                            "nodeProperties": ["id"],
                            "distinctOutgoingEdgeLabels": ["e1"],
                        },
                        # 500 B-nodes each emit >=1 e2  → min(e2) >= 500
                        {
                            "count": 500,
                            "nodeProperties": ["id"],
                            "distinctOutgoingEdgeLabels": ["e2"],
                        },
                        # 2400 C-nodes each emit >=1 e3 → min(e3) >= 2400
                        {
                            "count": 2400,
                            "nodeProperties": ["id"],
                            "distinctOutgoingEdgeLabels": ["e3"],
                        },
                    ],
                    "edgeStructures": [],  # all edges are zero-property
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
                        "nodeLabels": ["A", "B", "C"],
                        "edgeLabels": ["e1", "e2", "e3"],
                        "nodeLabelDetails": {
                            "A": {"properties": {"id": ["String"]}},
                            "B": {"properties": {"id": ["String"]}},
                            "C": {"properties": {"id": ["String"]}},
                        },
                        "edgeLabelDetails": {
                            "e1": {"properties": {}},
                            "e2": {"properties": {}},
                            "e3": {"properties": {}},
                        },
                        "labelTriples": [
                            {"~type": "e1", "~from": "A", "~to": "B"},
                            {"~type": "e2", "~from": "B", "~to": "C"},
                            {"~type": "e3", "~from": "C", "~to": "A"},
                        ],
                    }
                }
            ]
        }
        summary = PGSummary.from_json(summary_json)
        schema = PGSchema.from_json(schema_json)
        return correlate_pg(summary, schema)

    def test_e1_bound(self, result):
        b = result.edge_label_bounds["e1"]
        assert not b.is_exact
        assert b.min_count == 100
        # max = total (10000) - others' floors (500 + 2400) = 7100
        assert b.max_count == 7100
        assert b.source_labels == ("A",)

    def test_e2_bound(self, result):
        b = result.edge_label_bounds["e2"]
        assert not b.is_exact
        assert b.min_count == 500
        # max = 10000 - (100 + 2400) = 7500
        assert b.max_count == 7500
        assert b.source_labels == ("B",)

    def test_e3_bound(self, result):
        b = result.edge_label_bounds["e3"]
        assert not b.is_exact
        assert b.min_count == 2400
        # max = 10000 - (100 + 500) = 9400
        assert b.max_count == 9400
        assert b.source_labels == ("C",)

    def test_max_of_bounds_sum_to_total(self, result):
        # A weaker check: for any label, min + sum(other mins) <= total.
        # And max = total - sum(other mins), so min + max_of_others == total.
        total = 10000
        bounds = list(result.edge_label_bounds.values())
        for label_bound in bounds:
            other_mins = sum(b.min_count for b in bounds if b.label != label_bound.label)
            assert label_bound.max_count == total - other_mins


# ---------------------------------------------------------------------------
# Node label bounds + edges-by-target + consistency
# ---------------------------------------------------------------------------


class TestNodeLabelBoundsAirRoutes:
    """Node-label bounds against the air-routes fixture."""

    @pytest.fixture
    def result(self, air_routes_summary, air_routes_schema):
        return correlate_pg(air_routes_summary, air_routes_schema)

    def test_airport_has_range_from_4way_ambiguity(self, result):
        b = result.node_label_bounds["airport"]
        # 3,471 (structure 1) + 32 (structure 4) = 3,503 exact.
        # +83 from the 4-way ambiguous group containing airport.
        assert b.min_count == 3503
        assert b.max_count == 3586
        assert b.exact_structures == 2

    def test_version_ranges_up_to_84(self, result):
        b = result.node_label_bounds["version"]
        assert b.min_count == 1
        # +83 from the 4-way ambiguous group.
        assert b.max_count == 84

    def test_continent_and_country_share_headroom(self, result):
        # Both should have min=0 and max = 244 (161 from 2-way + 83 from 4-way).
        for label in ("continent", "country"):
            b = result.node_label_bounds[label]
            assert b.min_count == 0
            assert b.max_count == 244


class TestEdgesByTargetAirRoutes:
    """Edges-by-target derivation against air-routes."""

    @pytest.fixture
    def result(self, air_routes_summary, air_routes_schema):
        return correlate_pg(air_routes_summary, air_routes_schema)

    def test_airport_receives_all_edges(self, result):
        # Both route (airport→airport) and contains (country/continent→airport)
        # target airport. Sum is total edges (51,300).
        target = result.edges_by_target_label["airport"]
        assert target.min_count == 51300
        assert target.max_count == 51300
        assert target.is_exact
        assert set(target.contributing_edge_labels) == {"route", "contains"}

    def test_non_target_labels_have_zero_incoming(self, result):
        for label in ("continent", "country", "version"):
            target = result.edges_by_target_label[label]
            assert target.min_count == 0
            assert target.max_count == 0
            assert target.contributing_edge_labels == ()


class TestEdgesByTargetIdentityShape:
    """Edges-by-target derivation for a multi-source graph shape."""

    def test_website_target_sums_visited_and_links_to(self):
        # Minimal identity-graph shape
        summary_json = {
            "status": "200 OK",
            "payload": {
                "version": "v1",
                "lastStatisticsComputationTime": "2026-08-09T00:00:00Z",
                "graphSummary": {
                    "numNodes": 1000,
                    "numEdges": 10000,
                    "numNodeLabels": 2,
                    "numEdgeLabels": 2,
                    "nodeLabels": ["src", "tgt"],
                    "edgeLabels": ["e1", "e2"],
                    "numNodeProperties": 0,
                    "numEdgeProperties": 0,
                    "nodeProperties": [],
                    "edgeProperties": [],
                    "totalNodePropertyValues": 0,
                    "totalEdgePropertyValues": 0,
                    "nodeStructures": [
                        {
                            "count": 100,
                            "nodeProperties": ["s"],
                            "distinctOutgoingEdgeLabels": ["e1", "e2"],
                        },
                        {"count": 900, "nodeProperties": ["t"], "distinctOutgoingEdgeLabels": []},
                    ],
                    "edgeStructures": [
                        {"count": 5000, "edgeProperties": ["p"]},
                    ],
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
                        "nodeLabels": ["src", "tgt"],
                        "edgeLabels": ["e1", "e2"],
                        "nodeLabelDetails": {
                            "src": {"properties": {"s": ["String"]}},
                            "tgt": {"properties": {"t": ["String"]}},
                        },
                        "edgeLabelDetails": {
                            "e1": {"properties": {"p": ["String"]}},
                            "e2": {"properties": {}},
                        },
                        "labelTriples": [
                            {"~type": "e1", "~from": "src", "~to": "tgt"},
                            {"~type": "e2", "~from": "src", "~to": "tgt"},
                        ],
                    }
                }
            ]
        }
        summary = PGSummary.from_json(summary_json)
        schema = PGSchema.from_json(schema_json)
        result = correlate_pg(summary, schema)

        tgt_target = result.edges_by_target_label["tgt"]
        # e1 exact = 5000, e2 residual = 5000 (only 0-prop label → exact).
        assert tgt_target.min_count == 10000
        assert tgt_target.max_count == 10000
        assert tgt_target.is_exact
        assert set(tgt_target.contributing_edge_labels) == {"e1", "e2"}

        src_target = result.edges_by_target_label["src"]
        # No edge type targets src.
        assert src_target.min_count == 0
        assert src_target.max_count == 0


class TestSumValidation:
    """Post-scan validation of sum(label counts) == totals."""

    def test_validate_scan_totals_passes_on_consistent_data(
        self, air_routes_summary, air_routes_schema
    ):
        from neptune_schema_stats.correlator import validate_scan_totals
        from neptune_schema_stats.scan import (
            NodeCountScan,
            ScanPlan,
            ScanResults,
            apply_scan,
        )

        result = correlate_pg(air_routes_summary, air_routes_schema)
        # Simulate a scan that resolves all bounds consistently.
        scan = ScanResults(
            plan=ScanPlan(
                node_labels_to_query=("airport", "continent", "country", "version"),
                edge_labels_to_query=(),
            ),
            node_scans=(
                NodeCountScan(label="airport", exact_count=3503),
                NodeCountScan(label="continent", exact_count=7),
                NodeCountScan(label="country", exact_count=237),
                NodeCountScan(label="version", exact_count=1),
            ),
        )
        updated = apply_scan(result, scan, summary=air_routes_summary)
        # Sum: 7 + 237 + 1 + 3503 = 3748 (matches numNodes).
        sum_warnings = [
            w for w in updated.consistency_warnings if w.kind == "node_label_sum_mismatch"
        ]
        assert sum_warnings == []
        # Also check the validation helper directly.
        assert validate_scan_totals(summary=air_routes_summary, result=updated) == ()

    def test_validate_scan_totals_flags_inconsistent_node_sum(
        self, air_routes_summary, air_routes_schema
    ):
        from neptune_schema_stats.scan import (
            NodeCountScan,
            ScanPlan,
            ScanResults,
            apply_scan,
        )

        result = correlate_pg(air_routes_summary, air_routes_schema)
        # Simulate a scan whose totals sum to *more* than num_nodes (as
        # would happen with multi-labeled nodes counted under each label).
        scan = ScanResults(
            plan=ScanPlan(
                node_labels_to_query=("airport", "continent", "country", "version"),
                edge_labels_to_query=(),
            ),
            node_scans=(
                NodeCountScan(label="airport", exact_count=3503),
                NodeCountScan(label="continent", exact_count=100),  # inflated
                NodeCountScan(label="country", exact_count=237),
                NodeCountScan(label="version", exact_count=1),
            ),
        )
        updated = apply_scan(result, scan, summary=air_routes_summary)
        sum_warnings = [
            w for w in updated.consistency_warnings if w.kind == "node_label_sum_mismatch"
        ]
        assert len(sum_warnings) == 1
        w = sum_warnings[0]
        assert w.expected == 3748
        # 100 + 237 + 1 + 3503 = 3841
        assert w.actual == 3841


class TestConsistencyWarnings:
    """Verifies the correlator surfaces summary-vs-structure mismatches."""

    def test_property_fill_mismatch_flagged(self):
        # Structurally-impossible case: sum of node-structure counts containing
        # 'title' is LARGER than the graph-wide value count. This would only
        # happen with a corrupted summary, but it's the one direction that
        # can't be explained by Neptune's multi-valued vertex properties.
        summary_json = {
            "status": "200 OK",
            "payload": {
                "version": "v1",
                "lastStatisticsComputationTime": "2026-08-09T00:00:00Z",
                "graphSummary": {
                    "numNodes": 100,
                    "numEdges": 0,
                    "numNodeLabels": 1,
                    "numEdgeLabels": 0,
                    "nodeLabels": ["A"],
                    "edgeLabels": [],
                    "numNodeProperties": 1,
                    "numEdgeProperties": 0,
                    "nodeProperties": [{"title": 50}],
                    "edgeProperties": [],
                    "totalNodePropertyValues": 100,
                    "totalEdgePropertyValues": 0,
                    "nodeStructures": [
                        {
                            "count": 100,
                            "nodeProperties": ["title"],
                            "distinctOutgoingEdgeLabels": [],
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
                        "edgeLabels": [],
                        "nodeLabelDetails": {"A": {"properties": {"title": ["String"]}}},
                        "edgeLabelDetails": {},
                        "labelTriples": [],
                    }
                }
            ]
        }
        summary = PGSummary.from_json(summary_json)
        schema = PGSchema.from_json(schema_json)
        result = correlate_pg(summary, schema)

        # Should be exactly one warning for the title mismatch.
        warnings = [
            w for w in result.consistency_warnings if w.kind == "node_property_fill_mismatch"
        ]
        assert len(warnings) == 1
        w = warnings[0]
        assert w.subject == "title"
        assert w.expected == 50
        assert w.actual == 100

    def test_no_warnings_on_consistent_summary(self, air_routes_summary, air_routes_schema):
        result = correlate_pg(air_routes_summary, air_routes_schema)
        assert result.consistency_warnings == ()

    def test_multi_valued_property_does_not_warn(self):
        # nodeProperties count (values) > structure sum (nodes with property).
        # This is the normal multi-valued case in Neptune's PG data model
        # (one vertex can carry several 'title' values). Should be silent.
        summary_json = {
            "status": "200 OK",
            "payload": {
                "version": "v1",
                "lastStatisticsComputationTime": "2026-08-09T00:00:00Z",
                "graphSummary": {
                    "numNodes": 50,
                    "numEdges": 0,
                    "numNodeLabels": 1,
                    "numEdgeLabels": 0,
                    "nodeLabels": ["A"],
                    "edgeLabels": [],
                    "numNodeProperties": 1,
                    "numEdgeProperties": 0,
                    # 100 title values across 50 nodes — 50 nodes with ~2 values each
                    "nodeProperties": [{"title": 100}],
                    "edgeProperties": [],
                    "totalNodePropertyValues": 100,
                    "totalEdgePropertyValues": 0,
                    "nodeStructures": [
                        {
                            "count": 50,
                            "nodeProperties": ["title"],
                            "distinctOutgoingEdgeLabels": [],
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
                        "edgeLabels": [],
                        "nodeLabelDetails": {"A": {"properties": {"title": ["String"]}}},
                        "edgeLabelDetails": {},
                        "labelTriples": [],
                    }
                }
            ]
        }
        summary = PGSummary.from_json(summary_json)
        schema = PGSchema.from_json(schema_json)
        result = correlate_pg(summary, schema)

        # No node_property_fill_mismatch — the value/node delta is expected.
        title_warnings = [
            w
            for w in result.consistency_warnings
            if w.kind == "node_property_fill_mismatch" and w.subject == "title"
        ]
        assert title_warnings == []


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------


class TestCorrelateGuardRails:
    def test_raises_when_schema_not_usable(self, pg_summary_json, pg_schema_not_started_json):
        summary = PGSummary.from_json(pg_summary_json)
        schema = PGSchema.from_json(pg_schema_not_started_json)
        with pytest.raises(ValueError, match="not usable"):
            correlate_pg(summary, schema)
