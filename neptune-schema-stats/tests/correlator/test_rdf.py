"""Tests for the RDF correlator."""

from __future__ import annotations

import pytest

from neptune_schema_stats.correlator.rdf import (
    ClassCount,
    PredicateCategory,
    classify_predicate,
    correlate_rdf,
    result_to_jsonable,
)
from neptune_schema_stats.models import RDFSummary


@pytest.fixture
def rdf_summary(rdf_summary_json) -> RDFSummary:
    return RDFSummary.from_json(rdf_summary_json)


class TestClassifyPredicate:
    def test_rdf_type(self):
        assert (
            classify_predicate("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
            == PredicateCategory.RDF
        )

    def test_rdfs_label(self):
        assert (
            classify_predicate("http://www.w3.org/2000/01/rdf-schema#label")
            == PredicateCategory.RDFS
        )

    def test_owl_class(self):
        assert classify_predicate("http://www.w3.org/2002/07/owl#Class") == PredicateCategory.OWL

    def test_neptune_pg_uris_are_treated_as_custom(self):
        """PG-shape URIs (/datatypeProperty/, /objectProperty/) get no
        special treatment — they're custom like any other user URI. The
        URI shape doesn't reliably distinguish data vs object properties."""
        assert (
            classify_predicate("http://kelvinlawrence.net/air-routes/datatypeProperty/dist")
            == PredicateCategory.CUSTOM
        )
        assert (
            classify_predicate("http://kelvinlawrence.net/air-routes/objectProperty/route")
            == PredicateCategory.CUSTOM
        )

    def test_custom(self):
        assert classify_predicate("http://example.org/property/x") == PredicateCategory.CUSTOM


class TestCorrelateAirRoutesRDF:
    """Correlator behavior against the air-routes RDF fixture."""

    @pytest.fixture
    def result(self, rdf_summary):
        return correlate_rdf(rdf_summary)

    def test_typed_and_untyped_counts_match_predicates(self, rdf_summary, result):
        # rdf:type occurrence in predicates == typed subject count
        typed_from_pred = rdf_summary.predicates["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
        assert result.num_typed_subjects == typed_from_pred
        assert result.num_typed_subjects == 3747

    def test_untyped_subjects_counted(self, result):
        # The 50,656 'dist-only' subjects are untyped.
        assert result.num_untyped_subjects == 50656

    def test_total_subjects_covered_by_structures(self, rdf_summary, result):
        total = sum(s.count for s in result.subject_structures)
        assert total == rdf_summary.num_distinct_subjects

    def test_route_and_dist_predicates_present_in_stats(self, result):
        uris = {p.uri for p in result.predicate_stats}
        assert "http://kelvinlawrence.net/air-routes/objectProperty/route" in uris
        assert "http://kelvinlawrence.net/air-routes/datatypeProperty/dist" in uris

    def test_predicates_sorted_by_count_desc(self, result):
        counts = [p.occurrence_count for p in result.predicate_stats]
        assert counts == sorted(counts, reverse=True)

    def test_no_class_counts_when_probe_not_run(self, result):
        assert result.class_counts == ()
        assert result.num_typed_subjects_by_class == 0


class TestCorrelateWithClassCounts:
    def test_class_counts_populated(self, rdf_summary):
        class_counts = {
            "http://kelvinlawrence.net/air-routes/class/Airport": 3502,
            "http://kelvinlawrence.net/air-routes/class/Country": 237,
            "http://kelvinlawrence.net/air-routes/class/Continent": 7,
            "http://kelvinlawrence.net/air-routes/class/Version": 1,
        }
        result = correlate_rdf(rdf_summary, class_counts=class_counts)
        assert len(result.class_counts) == 4
        assert isinstance(result.class_counts[0], ClassCount)
        # Sorted by count descending.
        counts = [cc.subject_count for cc in result.class_counts]
        assert counts == sorted(counts, reverse=True)
        # Sum matches typed subjects (no multi-typing in air-routes).
        assert result.num_typed_subjects_by_class == 3502 + 237 + 7 + 1

    def test_class_counts_sum_matches_typed_when_single_typed(self, rdf_summary):
        class_counts = {
            "http://kelvinlawrence.net/air-routes/class/Airport": 3502,
            "http://kelvinlawrence.net/air-routes/class/Country": 237,
            "http://kelvinlawrence.net/air-routes/class/Continent": 7,
            "http://kelvinlawrence.net/air-routes/class/Version": 1,
        }
        result = correlate_rdf(rdf_summary, class_counts=class_counts)
        assert result.num_typed_subjects_by_class == result.num_typed_subjects


class TestJsonSerialization:
    def test_result_to_jsonable_shape(self, rdf_summary):
        result = correlate_rdf(rdf_summary)
        payload = result_to_jsonable(result)
        assert "predicate_stats" in payload
        assert "subject_structures" in payload
        assert "class_counts" in payload
        assert "totals" in payload
        assert payload["totals"]["num_typed_subjects"] == 3747
