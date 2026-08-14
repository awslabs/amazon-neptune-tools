"""Tests for the SPARQL class-count probe."""

from __future__ import annotations

from unittest.mock import MagicMock

from neptune_schema_stats.client.sparql import (
    _parse_class_count_bindings,
    sparql_class_counts,
)


class TestParseBindings:
    def test_empty_results(self):
        assert (
            _parse_class_count_bindings({"head": {"vars": []}, "results": {"bindings": []}}) == {}
        )

    def test_missing_results_key(self):
        assert _parse_class_count_bindings({}) == {}

    def test_single_binding(self):
        body = {
            "head": {"vars": ["cls", "c"]},
            "results": {
                "bindings": [
                    {
                        "cls": {"type": "uri", "value": "http://example.org/Foo"},
                        "c": {"type": "literal", "value": "42"},
                    }
                ]
            },
        }
        assert _parse_class_count_bindings(body) == {"http://example.org/Foo": 42}

    def test_multiple_bindings(self):
        body = {
            "results": {
                "bindings": [
                    {
                        "cls": {"type": "uri", "value": "http://example.org/A"},
                        "c": {"type": "literal", "value": "100"},
                    },
                    {
                        "cls": {"type": "uri", "value": "http://example.org/B"},
                        "c": {"type": "literal", "value": "200"},
                    },
                ]
            }
        }
        result = _parse_class_count_bindings(body)
        assert result == {"http://example.org/A": 100, "http://example.org/B": 200}

    def test_skips_incomplete_bindings(self):
        body = {
            "results": {
                "bindings": [
                    {"cls": {"value": "http://example.org/A"}},  # missing 'c'
                    {"c": {"value": "10"}},  # missing 'cls'
                    {
                        "cls": {"value": "http://example.org/B"},
                        "c": {"value": "5"},
                    },
                ]
            }
        }
        assert _parse_class_count_bindings(body) == {"http://example.org/B": 5}

    def test_skips_bindings_with_non_numeric_count(self):
        body = {
            "results": {
                "bindings": [
                    {
                        "cls": {"value": "http://example.org/A"},
                        "c": {"value": "not-a-number"},
                    }
                ]
            }
        }
        assert _parse_class_count_bindings(body) == {}


class TestSparqlClassCounts:
    def test_query_sent_to_sparql_endpoint(self):
        mock_client = MagicMock()
        mock_client.sparql_query.return_value = {
            "results": {
                "bindings": [
                    {
                        "cls": {"value": "http://example.org/A"},
                        "c": {"value": "10"},
                    }
                ]
            }
        }
        result = sparql_class_counts(mock_client)
        assert result == {"http://example.org/A": 10}
        # Verify the query was sent through sparql_query
        mock_client.sparql_query.assert_called_once()
        args, _ = mock_client.sparql_query.call_args
        query = args[0]
        assert "SELECT" in query
        assert "GROUP BY ?cls" in query
