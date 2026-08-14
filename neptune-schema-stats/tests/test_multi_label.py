"""Tests for the multi-label probe module."""

from __future__ import annotations

from unittest.mock import patch

from neptune_schema_stats.client.base import NeptuneClient
from neptune_schema_stats.multi_label import (
    MAX_UNION_BRANCHES,
    MultiLabelPair,
    _build_existence_query,
    _quote_label,
    probe_multi_label_pairs,
)


class TestQuoteLabel:
    def test_wraps_in_backticks(self):
        assert _quote_label("website") == "`website`"

    def test_escapes_embedded_backticks(self):
        assert _quote_label("weird`name") == "`weird``name`"


class TestBuildExistenceQuery:
    def test_single_branch(self):
        query = _build_existence_query([("A", "B")], index_offset=0)
        assert query == "MATCH (n:`A`:`B`) WITH n LIMIT 1 RETURN 0 AS idx"

    def test_multi_branch_union_all(self):
        query = _build_existence_query([("A", "B"), ("B", "C"), ("A", "C")], index_offset=0)
        assert query.count("UNION ALL") == 2
        assert "MATCH (n:`A`:`B`) WITH n LIMIT 1 RETURN 0 AS idx" in query
        assert "MATCH (n:`B`:`C`) WITH n LIMIT 1 RETURN 1 AS idx" in query
        assert "MATCH (n:`A`:`C`) WITH n LIMIT 1 RETURN 2 AS idx" in query

    def test_index_offset_preserves_pair_mapping(self):
        # When we chunk large queries, the second chunk must continue
        # numbering from where the first left off.
        query = _build_existence_query([("D", "E")], index_offset=42)
        assert "RETURN 42 AS idx" in query


class TestProbeMultiLabelPairs:
    def test_fewer_than_two_labels_short_circuits(self):
        client = NeptuneClient("example.test")
        result = probe_multi_label_pairs(client, ["only_one"])
        assert result.pairs_checked == 0
        assert result.hits == ()
        assert not result.any_multi_label

    def test_all_pairs_empty_returns_no_hits(self):
        client = NeptuneClient("example.test")
        # Existence probe returns no rows → no hits.
        empty_response = {"results": []}
        with patch.object(client, "execute_cypher", return_value=empty_response) as post:
            result = probe_multi_label_pairs(client, ["A", "B", "C"])

        # Only the existence probe was issued — no per-pair count queries.
        assert post.call_count == 1
        assert result.pairs_checked == 3  # C(3, 2)
        assert result.hits == ()
        assert result.total_pair_intersections == 0

    def test_two_pairs_hit_run_targeted_count_queries(self):
        client = NeptuneClient("example.test")
        # First call: existence probe. Rows 0 and 2 match — i.e. pairs
        # (A, B) and (B, C) after sorting. Row 1, (A, C), does not match.
        existence_response = {
            "results": [
                {"idx": 0},
                {"idx": 2},
            ]
        }
        count_response_ab = {"results": [{"c": 1234}]}
        count_response_bc = {"results": [{"c": 42}]}

        with patch.object(
            client,
            "execute_cypher",
            side_effect=[
                existence_response,
                count_response_ab,
                count_response_bc,
            ],
        ) as post:
            result = probe_multi_label_pairs(client, ["C", "A", "B"])

        # 1 existence probe + 2 count follow-ups.
        assert post.call_count == 3
        assert result.pairs_checked == 3
        assert len(result.hits) == 2
        assert result.hits[0] == MultiLabelPair(labels=("A", "B"), node_count=1234)
        assert result.hits[1] == MultiLabelPair(labels=("B", "C"), node_count=42)
        assert result.any_multi_label
        assert result.total_pair_intersections == 1276

    def test_labels_are_deduplicated_and_sorted(self):
        client = NeptuneClient("example.test")
        with patch.object(client, "execute_cypher", return_value={"results": []}) as post:
            probe_multi_label_pairs(client, ["Z", "A", "A", "M"])
        # De-duplicated labels sorted → ["A", "M", "Z"]; pairs (A, M), (A, Z), (M, Z).
        query = post.call_args_list[0][0][0]
        assert "(n:`A`:`M`)" in query
        assert "(n:`A`:`Z`)" in query
        assert "(n:`M`:`Z`)" in query

    def test_chunks_large_pair_counts(self):
        client = NeptuneClient("example.test")
        # Build a label list yielding more pairs than MAX_UNION_BRANCHES.
        # For MAX=200 → labels count k where C(k, 2) > 200 → k >= 22.
        labels = [f"L{i:03d}" for i in range(22)]
        # C(22, 2) = 231; each chunk of MAX_UNION_BRANCHES=200 → 2 chunks.
        with patch.object(client, "execute_cypher", return_value={"results": []}) as post:
            probe_multi_label_pairs(client, labels)
        # Two chunked existence probes; no follow-up counts because all empty.
        assert post.call_count == 2

    def test_chunk_offsets_map_hits_back_correctly(self):
        client = NeptuneClient("example.test")
        labels = [f"L{i:03d}" for i in range(22)]
        pairs_total = 231  # C(22, 2)
        # Simulate: chunk 1 (idx 0..199) has one hit at idx 100; chunk 2
        # (idx 200..230) has one hit at idx 210. Count follow-ups return 5 and 7.
        with patch.object(
            client,
            "execute_cypher",
            side_effect=[
                {"results": [{"idx": 100}]},
                {"results": [{"idx": 210}]},
                {"results": [{"c": 5}]},
                {"results": [{"c": 7}]},
            ],
        ) as post:
            result = probe_multi_label_pairs(client, labels)

        assert post.call_count == 4  # 2 chunks + 2 counts
        assert result.pairs_checked == pairs_total
        assert len(result.hits) == 2
        assert result.hits[0].node_count == 5
        assert result.hits[1].node_count == 7


class TestMaxUnionBranches:
    def test_constant_is_positive(self):
        # Sanity guard: chunking assumes at least 1.
        assert MAX_UNION_BRANCHES >= 1
