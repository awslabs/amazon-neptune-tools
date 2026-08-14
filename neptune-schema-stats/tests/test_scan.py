"""Tests for the count scan tier."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from neptune_schema_stats.client.base import NeptuneClientError
from neptune_schema_stats.correlator import (
    AmbiguousNodeGroup,
    NodeLabelBound,
    correlate_pg,
)
from neptune_schema_stats.models import PGNodeStructure
from neptune_schema_stats.scan import (
    NodeCountScan,
    ScanPlan,
    ScanResults,
    apply_scan,
    execute_scan,
    plan_scan,
    query_edge_label_count,
    query_node_label_count,
)

# ---------------------------------------------------------------------------
# plan_scan
# ---------------------------------------------------------------------------


class TestPlanScanAirRoutes:
    """Plan-scan behavior against the air-routes fixture."""

    @pytest.fixture
    def result(self, air_routes_summary, air_routes_schema):
        return correlate_pg(air_routes_summary, air_routes_schema)

    def test_plan_targets_all_range_labels(self, result):
        plan = plan_scan(result)
        # 4-way ambiguity: airport | continent | country | version.
        # ALL four are queried directly (no derivation).
        assert set(plan.node_labels_to_query) == {
            "airport",
            "continent",
            "country",
            "version",
        }
        # Edges are all exact in air-routes.
        assert plan.edge_labels_to_query == ()

    def test_plan_skips_scan_when_no_ranges(self):
        # Construct a graph with no ambiguity: one exact node structure, one
        # unique-property edge type.
        from neptune_schema_stats.models import PGSchema, PGSummary

        summary = PGSummary.from_json(
            {
                "status": "200 OK",
                "payload": {
                    "version": "v1",
                    "lastStatisticsComputationTime": "2026-08-09T00:00:00Z",
                    "graphSummary": {
                        "numNodes": 5,
                        "numEdges": 0,
                        "numNodeLabels": 1,
                        "numEdgeLabels": 0,
                        "nodeLabels": ["A"],
                        "edgeLabels": [],
                        "numNodeProperties": 1,
                        "numEdgeProperties": 0,
                        "nodeProperties": [{"id": 5}],
                        "edgeProperties": [],
                        "totalNodePropertyValues": 5,
                        "totalEdgePropertyValues": 0,
                        "nodeStructures": [
                            {
                                "count": 5,
                                "nodeProperties": ["id"],
                                "distinctOutgoingEdgeLabels": [],
                            }
                        ],
                        "edgeStructures": [],
                    },
                },
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
                                "lastComputedTimestamp": "2026-08-09T00:00:00Z",
                                "progressPercentage": "100",
                            },
                            "nodeLabels": ["A"],
                            "edgeLabels": [],
                            "nodeLabelDetails": {"A": {"properties": {"id": ["String"]}}},
                            "edgeLabelDetails": {},
                            "labelTriples": [],
                        }
                    }
                ]
            }
        )
        result = correlate_pg(summary, schema)
        plan = plan_scan(result)
        assert plan.node_labels_to_query == ()
        assert plan.total_queries == 0


class TestPlanScanEdges:
    """Verifies edge-label scan planning when multiple labels share a signature."""

    def test_shared_signature_group_queries_all_labels(self):
        """Three edge labels sharing an empty signature: all are queried
        directly. No derivation."""
        from neptune_schema_stats.models import PGSchema, PGSummary

        summary = PGSummary.from_json(
            {
                "status": "200 OK",
                "payload": {
                    "version": "v1",
                    "lastStatisticsComputationTime": "2026-08-09T00:00:00Z",
                    "graphSummary": {
                        "numNodes": 3,
                        "numEdges": 1000,
                        "numNodeLabels": 3,
                        "numEdgeLabels": 3,
                        "nodeLabels": ["A", "B", "C"],
                        "edgeLabels": ["e1", "e2", "e3"],
                        "numNodeProperties": 0,
                        "numEdgeProperties": 0,
                        "nodeProperties": [],
                        "edgeProperties": [],
                        "totalNodePropertyValues": 0,
                        "totalEdgePropertyValues": 0,
                        "nodeStructures": [
                            {
                                "count": 1,
                                "nodeProperties": ["p"],
                                "distinctOutgoingEdgeLabels": ["e1"],
                            },
                            {
                                "count": 1,
                                "nodeProperties": ["p"],
                                "distinctOutgoingEdgeLabels": ["e2"],
                            },
                            {
                                "count": 1,
                                "nodeProperties": ["p"],
                                "distinctOutgoingEdgeLabels": ["e3"],
                            },
                        ],
                        "edgeStructures": [],
                    },
                },
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
                                "lastComputedTimestamp": "2026-08-09T00:00:00Z",
                                "progressPercentage": "100",
                            },
                            "nodeLabels": ["A", "B", "C"],
                            "edgeLabels": ["e1", "e2", "e3"],
                            "nodeLabelDetails": {
                                "A": {"properties": {"p": ["String"]}},
                                "B": {"properties": {"p": ["String"]}},
                                "C": {"properties": {"p": ["String"]}},
                            },
                            "edgeLabelDetails": {
                                "e1": {"properties": {}},
                                "e2": {"properties": {}},
                                "e3": {"properties": {}},
                            },
                            "labelTriples": [
                                {"~type": "e1", "~from": "A", "~to": "A"},
                                {"~type": "e2", "~from": "B", "~to": "B"},
                                {"~type": "e3", "~from": "C", "~to": "C"},
                            ],
                        }
                    }
                ]
            }
        )
        result = correlate_pg(summary, schema)
        # All 3 edge labels share the empty signature -> all have range bounds.
        plan = plan_scan(result)
        # All 3 are queried, none derived.
        assert len(plan.edge_labels_to_query) == 3


# ---------------------------------------------------------------------------
# apply_scan
# ---------------------------------------------------------------------------


class TestApplyScan:
    """Verifies apply_scan updates bounds correctly."""

    @pytest.fixture
    def result(self, air_routes_summary, air_routes_schema):
        return correlate_pg(air_routes_summary, air_routes_schema)

    def test_scanned_node_becomes_exact(self, result):
        plan = plan_scan(result)
        scan = ScanResults(
            plan=plan,
            node_scans=(
                NodeCountScan(label="airport", exact_count=3503),
                NodeCountScan(label="continent", exact_count=7),
                NodeCountScan(label="country", exact_count=237),
                NodeCountScan(label="version", exact_count=1),
            ),
        )
        updated = apply_scan(result, scan)
        for label in ("continent", "country", "version", "airport"):
            b = updated.node_label_bounds[label]
            assert b.is_exact, f"{label} should be exact after apply_scan"

    def test_unscanned_bounds_preserved(self, result):
        # Empty scan should leave everything unchanged.
        empty = ScanResults(plan=plan_scan(result))
        updated = apply_scan(result, empty)
        for label, bound in result.node_label_bounds.items():
            assert updated.node_label_bounds[label] == bound

    def test_edges_by_target_recomputed(self, result):
        """When edge bounds change via scan, edges_by_target should update."""
        empty = ScanResults(plan=plan_scan(result))
        updated = apply_scan(result, empty)
        assert updated.edges_by_target_label["airport"].max_count == 51300


# ---------------------------------------------------------------------------
# execute_scan
# ---------------------------------------------------------------------------


class TestExecuteScan:
    def test_executes_all_planned_queries(self):
        mock_client = MagicMock()
        mock_client.execute_cypher.side_effect = [
            {"results": [{"c": 7}]},
            {"results": [{"c": 237}]},
            {"results": [{"c": 1}]},
        ]
        plan = ScanPlan(
            node_labels_to_query=("continent", "country", "version"),
            edge_labels_to_query=(),
        )
        result = MagicMock()
        result.ambiguous_node_groups = ()
        result.edge_label_bounds = {}
        result.node_label_bounds = {}

        scan = execute_scan(mock_client, plan, result)
        assert len(scan.node_scans) == 3
        assert scan.node_scans[0].label == "continent"
        assert scan.node_scans[0].exact_count == 7

    def test_handles_query_failure_gracefully(self):
        mock_client = MagicMock()
        mock_client.execute_cypher.side_effect = NeptuneClientError("access denied")
        plan = ScanPlan(
            node_labels_to_query=("X",),
            edge_labels_to_query=(),
        )
        result = MagicMock()
        result.ambiguous_node_groups = ()
        result.edge_label_bounds = {}
        result.node_label_bounds = {}

        scan = execute_scan(mock_client, plan, result)
        assert scan.node_scans == ()
        assert scan.failed_node_labels == ("X",)


# ---------------------------------------------------------------------------
# Fill redistribution
# ---------------------------------------------------------------------------


class TestFillRedistribution:
    """apply_scan should redistribute ambiguous-structure property fills to
    the labels that absorbed them, for single-structure ambiguous groups."""

    def test_url_only_ambiguous_structure_attributed_after_scan(self):
        from neptune_schema_stats.correlator import correlate_pg
        from neptune_schema_stats.models import PGSchema, PGSummary

        summary = PGSummary.from_json(
            {
                "status": "200 OK",
                "payload": {
                    "version": "v1",
                    "lastStatisticsComputationTime": "2026-08-09T00:00:00Z",
                    "graphSummary": {
                        "numNodes": 1200,
                        "numEdges": 0,
                        "numNodeLabels": 2,
                        "numEdgeLabels": 0,
                        "nodeLabels": ["website", "websiteGroup"],
                        "edgeLabels": [],
                        "numNodeProperties": 0,
                        "numEdgeProperties": 0,
                        "nodeProperties": [],
                        "edgeProperties": [],
                        "totalNodePropertyValues": 0,
                        "totalEdgePropertyValues": 0,
                        "nodeStructures": [
                            {
                                "count": 200,
                                "nodeProperties": ["title", "url"],
                                "distinctOutgoingEdgeLabels": [],
                            },
                            {
                                "count": 100,
                                "nodeProperties": ["category", "categoryCode", "url"],
                                "distinctOutgoingEdgeLabels": [],
                            },
                            {
                                "count": 900,
                                "nodeProperties": ["url"],
                                "distinctOutgoingEdgeLabels": [],
                            },
                        ],
                        "edgeStructures": [],
                    },
                },
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
                                "lastComputedTimestamp": "2026-08-09T00:00:00Z",
                                "progressPercentage": "100",
                            },
                            "nodeLabels": ["website", "websiteGroup"],
                            "edgeLabels": [],
                            "nodeLabelDetails": {
                                "website": {
                                    "properties": {
                                        "title": ["String"],
                                        "url": ["String"],
                                    }
                                },
                                "websiteGroup": {
                                    "properties": {
                                        "category": ["String"],
                                        "categoryCode": ["String"],
                                        "url": ["String"],
                                    }
                                },
                            },
                            "edgeLabelDetails": {},
                            "labelTriples": [],
                        }
                    }
                ]
            }
        )
        result = correlate_pg(summary, schema)
        # Pre-scan attribution: website=200, websiteGroup=100, ambiguous=900.
        assert result.label_stats["website"].node_count == 200
        assert result.label_stats["websiteGroup"].node_count == 100
        # Simulate scan: both labels queried directly.
        scan = ScanResults(
            plan=ScanPlan(
                node_labels_to_query=("website", "websiteGroup"),
                edge_labels_to_query=(),
            ),
            node_scans=(
                NodeCountScan(label="website", exact_count=1100),
                NodeCountScan(label="websiteGroup", exact_count=100),
            ),
        )
        updated = apply_scan(result, scan)

        # Website count updated to 1,100 (200 exact + 900 from group).
        assert updated.label_stats["website"].node_count == 1100
        # url fills for website: 200 (from structure 1) + 900 (from structure 3) = 1100 (100%).
        assert updated.label_stats["website"].property_fill_counts["url"] == 1100
        # title fills for website: still 200 (structure 3 doesn't have title).
        assert updated.label_stats["website"].property_fill_counts["title"] == 200
        # websiteGroup unchanged.
        assert updated.label_stats["websiteGroup"].node_count == 100
        assert updated.label_stats["websiteGroup"].property_fill_counts["url"] == 100

    def test_multi_structure_ambiguous_group_leaves_fills_unchanged(self):
        from neptune_schema_stats.correlator import correlate_pg
        from neptune_schema_stats.models import PGSchema, PGSummary

        summary = PGSummary.from_json(
            {
                "status": "200 OK",
                "payload": {
                    "version": "v1",
                    "lastStatisticsComputationTime": "2026-08-09T00:00:00Z",
                    "graphSummary": {
                        "numNodes": 200,
                        "numEdges": 0,
                        "numNodeLabels": 2,
                        "numEdgeLabels": 0,
                        "nodeLabels": ["A", "B"],
                        "edgeLabels": [],
                        "numNodeProperties": 0,
                        "numEdgeProperties": 0,
                        "nodeProperties": [],
                        "edgeProperties": [],
                        "totalNodePropertyValues": 0,
                        "totalEdgePropertyValues": 0,
                        "nodeStructures": [
                            {
                                "count": 100,
                                "nodeProperties": ["p"],
                                "distinctOutgoingEdgeLabels": [],
                            },
                            {
                                "count": 100,
                                "nodeProperties": ["q"],
                                "distinctOutgoingEdgeLabels": [],
                            },
                        ],
                        "edgeStructures": [],
                    },
                },
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
                                "lastComputedTimestamp": "2026-08-09T00:00:00Z",
                                "progressPercentage": "100",
                            },
                            "nodeLabels": ["A", "B"],
                            "edgeLabels": [],
                            "nodeLabelDetails": {
                                "A": {"properties": {"p": ["String"], "q": ["String"]}},
                                "B": {"properties": {"p": ["String"], "q": ["String"]}},
                            },
                            "edgeLabelDetails": {},
                            "labelTriples": [],
                        }
                    }
                ]
            }
        )
        result = correlate_pg(summary, schema)
        assert result.ambiguous_node_groups
        group = result.ambiguous_node_groups[0]
        assert len(group.structures) == 2

        scan = ScanResults(
            plan=ScanPlan(node_labels_to_query=("A",), edge_labels_to_query=()),
            node_scans=(NodeCountScan(label="A", exact_count=150),),
        )
        updated = apply_scan(result, scan)
        # Counts still updated from bounds.
        assert updated.node_label_bounds["A"].min_count == 150
        # But property fills unchanged — multi-structure group is left alone.
        assert updated.label_stats["A"].property_fill_counts == {"p": 0, "q": 0}


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


class TestQueryHelpers:
    def test_query_node_label_count_uses_backtick_quoting(self):
        mock_client = MagicMock()
        mock_client.execute_cypher.return_value = {"results": [{"c": 42}]}
        c = query_node_label_count(mock_client, "airport")
        assert c == 42
        args, _ = mock_client.execute_cypher.call_args
        assert "MATCH (n:`airport`)" in args[0]

    def test_query_edge_label_count_uses_backtick_quoting(self):
        mock_client = MagicMock()
        mock_client.execute_cypher.return_value = {"results": [{"c": 10}]}
        c = query_edge_label_count(mock_client, "route")
        assert c == 10
        args, _ = mock_client.execute_cypher.call_args
        assert "MATCH ()-[r:`route`]->()" in args[0]

    def test_labels_with_backticks_escaped(self):
        mock_client = MagicMock()
        mock_client.execute_cypher.return_value = {"results": [{"c": 0}]}
        query_node_label_count(mock_client, "weird`label")
        args, _ = mock_client.execute_cypher.call_args
        assert "`weird``label`" in args[0]

    def test_zero_results_returns_zero(self):
        mock_client = MagicMock()
        mock_client.execute_cypher.return_value = {"results": []}
        assert query_node_label_count(mock_client, "X") == 0


# ---------------------------------------------------------------------------
# Direct-scan sanity checks (multi-label overlap defense)
# ---------------------------------------------------------------------------


class TestRejectedScannedCount:
    """When a direct scan returns a value that exceeds the label's pre-scan
    max (and no multi-label probe explains the excess), we reject it and
    keep the range bound."""

    def test_scanned_peer_exceeding_max_without_multi_label_context(self):
        from neptune_schema_stats.correlator import PGCorrelationResult

        ambig = AmbiguousNodeGroup(
            candidate_labels=("movie", "Artist"),
            structures=(
                PGNodeStructure(
                    count=925_797,
                    node_properties=(),
                    distinct_outgoing_edge_labels=(),
                ),
            ),
            total_count=925_797,
        )
        result = PGCorrelationResult(
            node_matches=(),
            edge_matches=(),
            label_stats={},
            edge_stats={},
            ambiguous_node_groups=(ambig,),
            unmapped_node_structures=(),
            implicit_empty_edges=None,
            node_label_bounds={
                "movie": NodeLabelBound(
                    label="movie", min_count=0, max_count=925_797, exact_structures=0
                ),
                "Artist": NodeLabelBound(
                    label="Artist", min_count=0, max_count=925_797, exact_structures=0
                ),
            },
            edge_label_bounds={},
        )
        scan = ScanResults(
            plan=ScanPlan(
                node_labels_to_query=("Artist",),
                edge_labels_to_query=(),
            ),
            node_scans=(NodeCountScan(label="Artist", exact_count=1_167_747),),
        )
        applied = apply_scan(result, scan)
        artist_bound = applied.node_label_bounds["Artist"]
        # Bound should NOT have been collapsed to 1,167,747 — original range kept
        assert artist_bound.min_count == 0
        assert artist_bound.max_count == 925_797
        # A consistency warning was emitted
        assert any(
            w.kind == "rejected_scanned_node_count" and w.subject == "Artist"
            for w in applied.consistency_warnings
        )


# ---------------------------------------------------------------------------
# Label partition (multi-label detection)
# ---------------------------------------------------------------------------


class TestLabelPartition:
    """When the multi-label probe detects overlaps, apply_scan should compute
    a discrete labelset partition instead of leaving the user with counts
    that overlap."""

    def _build_result_with_scans(
        self,
        *,
        artist_total: int,
        movie_total: int,
    ):
        """Build a correlation result where both labels have unambiguous
        structures matching their totals (so post-scan bounds are exact)."""
        from neptune_schema_stats.correlator import (
            LabelStats,
            PGCorrelationResult,
        )

        return PGCorrelationResult(
            node_matches=(),
            edge_matches=(),
            label_stats={
                "Artist": LabelStats(
                    label="Artist",
                    node_count=artist_total,
                    property_fill_counts={},
                    contributing_structures=1,
                ),
                "movie": LabelStats(
                    label="movie",
                    node_count=movie_total,
                    property_fill_counts={},
                    contributing_structures=1,
                ),
            },
            edge_stats={},
            ambiguous_node_groups=(),
            unmapped_node_structures=(),
            implicit_empty_edges=None,
            node_label_bounds={
                "Artist": NodeLabelBound(
                    label="Artist",
                    min_count=artist_total,
                    max_count=artist_total,
                    exact_structures=1,
                ),
                "movie": NodeLabelBound(
                    label="movie",
                    min_count=movie_total,
                    max_count=movie_total,
                    exact_structures=1,
                ),
            },
            edge_label_bounds={},
        )

    def test_partition_computed_when_multi_label_pair_detected(self):
        """Given Artist:movie overlap of 241,950 nodes, apply_scan should
        emit three partition entries: (Artist,), (movie,), and (Artist, movie)."""
        from neptune_schema_stats.multi_label import (
            MultiLabelPair,
            MultiLabelProbeResult,
        )

        result = self._build_result_with_scans(
            artist_total=1_167_747,
            movie_total=925_797,
        )
        # Scans returned the raw MATCH (n:L) counts (which include overlap).
        scan = ScanResults(
            plan=ScanPlan(node_labels_to_query=("Artist", "movie"), edge_labels_to_query=()),
            node_scans=(
                NodeCountScan(label="Artist", exact_count=1_167_747),
                NodeCountScan(label="movie", exact_count=925_797),
            ),
        )
        probe = MultiLabelProbeResult(
            pairs_checked=1,
            hits=(MultiLabelPair(labels=("Artist", "movie"), node_count=241_950),),
        )

        applied = apply_scan(result, scan, multi_label=probe)

        # The partition should have three entries
        by_labels = {e.labels: e.count for e in applied.label_partition}
        assert by_labels == {
            ("Artist",): 1_167_747 - 241_950,  # exclusive Artist = 925,797
            ("movie",): 925_797 - 241_950,  # exclusive movie = 683,847
            ("Artist", "movie"): 241_950,  # multi-labeled
        }
        # Sum equals union of all nodes with either label
        total = sum(e.count for e in applied.label_partition)
        assert total == 1_167_747 + 925_797 - 241_950  # = 1,851,594

    def test_no_partition_when_no_multi_labels(self):
        """When the probe finds nothing, no partition is computed."""
        from neptune_schema_stats.multi_label import MultiLabelProbeResult

        result = self._build_result_with_scans(artist_total=100, movie_total=200)
        scan = ScanResults(
            plan=ScanPlan(node_labels_to_query=("Artist", "movie"), edge_labels_to_query=()),
            node_scans=(
                NodeCountScan(label="Artist", exact_count=100),
                NodeCountScan(label="movie", exact_count=200),
            ),
        )
        probe = MultiLabelProbeResult(pairs_checked=1, hits=())

        applied = apply_scan(result, scan, multi_label=probe)
        assert applied.label_partition == ()

    def test_scan_exceeding_max_by_overlap_is_accepted(self):
        """A scan that exceeds pre-scan max_count by the known multi-label
        overlap should NOT be rejected — the excess is expected and
        accounted for by the partition."""
        from neptune_schema_stats.correlator import (
            AmbiguousNodeGroup,
            LabelStats,
            PGCorrelationResult,
        )
        from neptune_schema_stats.models import PGNodeStructure
        from neptune_schema_stats.multi_label import (
            MultiLabelPair,
            MultiLabelProbeResult,
        )

        # Pre-scan bounds have Artist and movie in an ambiguous group with
        # group_total = 925,797 (based on characteristic-set analysis).
        # Their bound max = 925,797. Post-multi-label scan returns 1,167,747
        # for Artist — exceeds max by 241,950. That excess exactly matches
        # the multi-label overlap.
        ambig = AmbiguousNodeGroup(
            candidate_labels=("Artist", "movie"),
            structures=(
                PGNodeStructure(
                    count=925_797,
                    node_properties=(),
                    distinct_outgoing_edge_labels=(),
                ),
            ),
            total_count=925_797,
        )
        result = PGCorrelationResult(
            node_matches=(),
            edge_matches=(),
            label_stats={
                "Artist": LabelStats(
                    label="Artist",
                    node_count=0,
                    property_fill_counts={},
                    contributing_structures=0,
                ),
                "movie": LabelStats(
                    label="movie",
                    node_count=0,
                    property_fill_counts={},
                    contributing_structures=0,
                ),
            },
            edge_stats={},
            ambiguous_node_groups=(ambig,),
            unmapped_node_structures=(),
            implicit_empty_edges=None,
            node_label_bounds={
                "Artist": NodeLabelBound(
                    label="Artist", min_count=0, max_count=925_797, exact_structures=0
                ),
                "movie": NodeLabelBound(
                    label="movie", min_count=0, max_count=925_797, exact_structures=0
                ),
            },
            edge_label_bounds={},
        )
        scan = ScanResults(
            plan=ScanPlan(node_labels_to_query=("Artist",), edge_labels_to_query=()),
            node_scans=(NodeCountScan(label="Artist", exact_count=1_167_747),),
        )
        probe = MultiLabelProbeResult(
            pairs_checked=1,
            hits=(MultiLabelPair(labels=("Artist", "movie"), node_count=241_950),),
        )

        applied = apply_scan(result, scan, multi_label=probe)

        # Artist's bound should now be exact at 1,167,747 (the raw scan value)
        artist_bound = applied.node_label_bounds["Artist"]
        assert artist_bound.is_exact
        assert artist_bound.min_count == 1_167_747
        # And the partition entry for exclusive Artist is 1,167,747 - 241,950
        artist_exclusive = next(e for e in applied.label_partition if e.labels == ("Artist",))
        assert artist_exclusive.count == 925_797

    def test_three_way_overlap_produces_negative_exclusive_warning(self):
        """When pairwise probes miss a 3+ way overlap, one label's exclusive
        count would come out negative. We emit a consistency warning
        instead of silently reporting a nonsensical number."""
        from neptune_schema_stats.multi_label import (
            MultiLabelPair,
            MultiLabelProbeResult,
        )

        # Artist total = 100. Pairs (Artist, movie) = 60, (Artist, director) = 60.
        # Sum of pair counts = 120 > Artist total. Some Artists must be
        # simultaneously in both pairs (i.e., :Artist:movie:director).
        # Naive exclusive = 100 - 60 - 60 = -20.
        result = self._build_result_with_scans(artist_total=100, movie_total=60)
        # Add director too
        from neptune_schema_stats.correlator import LabelStats

        result.label_stats["director"] = LabelStats(
            label="director",
            node_count=60,
            property_fill_counts={},
            contributing_structures=1,
        )
        result.node_label_bounds["director"] = NodeLabelBound(
            label="director", min_count=60, max_count=60, exact_structures=1
        )
        scan = ScanResults(
            plan=ScanPlan(
                node_labels_to_query=("Artist", "movie", "director"),
                edge_labels_to_query=(),
            ),
            node_scans=(
                NodeCountScan(label="Artist", exact_count=100),
                NodeCountScan(label="movie", exact_count=60),
                NodeCountScan(label="director", exact_count=60),
            ),
        )
        probe = MultiLabelProbeResult(
            pairs_checked=3,
            hits=(
                MultiLabelPair(labels=("Artist", "movie"), node_count=60),
                MultiLabelPair(labels=("Artist", "director"), node_count=60),
            ),
        )

        applied = apply_scan(result, scan, multi_label=probe)

        # Artist should NOT appear as a singleton in the partition (negative)
        singletons = [e for e in applied.label_partition if not e.is_multi]
        singleton_labels = {e.labels[0] for e in singletons}
        assert "Artist" not in singleton_labels
        # But we should have a consistency warning explaining why
        assert any(
            w.kind == "label_partition_negative_exclusive" and w.subject == "Artist"
            for w in applied.consistency_warnings
        )
