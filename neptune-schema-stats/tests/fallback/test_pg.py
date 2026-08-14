"""Tests for the pg_schema fallback flow."""

from __future__ import annotations

from unittest.mock import MagicMock

from neptune_schema_stats.client.base import (
    NeptuneAPIError,
    NeptuneClientError,
    NeptuneHTTPError,
)
from neptune_schema_stats.fallback.pg import (
    FallbackResult,
    fetch_label_counts,
    is_pg_schema_unavailable_error,
)
from neptune_schema_stats.models import PGSummary


class TestPgSchemaUnavailableDetection:
    def test_detects_unknown_procedure_error(self):
        exc = NeptuneHTTPError(
            NeptuneAPIError(
                code="MalformedQueryException",
                detailed_message="unknown procedure neptune.graph.pg_schema",
            )
        )
        assert is_pg_schema_unavailable_error(exc)

    def test_ignores_other_malformed_query_errors(self):
        exc = NeptuneHTTPError(
            NeptuneAPIError(
                code="MalformedQueryException",
                detailed_message="syntax error near 'MATCH'",
            )
        )
        assert not is_pg_schema_unavailable_error(exc)

    def test_ignores_non_http_error(self):
        assert not is_pg_schema_unavailable_error(NeptuneClientError("network"))
        assert not is_pg_schema_unavailable_error(ValueError("x"))


class TestFetchLabelCounts:
    def test_queries_every_node_and_edge_label(self):
        mock_client = MagicMock()
        # 2 node label queries + 2 edge label queries; each returns count 42.
        mock_client.execute_cypher.side_effect = [
            {"results": [{"c": 10}]},  # airport
            {"results": [{"c": 20}]},  # country
            {"results": [{"c": 30}]},  # route
            {"results": [{"c": 40}]},  # contains
        ]
        result = fetch_label_counts(
            mock_client,
            node_labels=["airport", "country"],
            edge_labels=["route", "contains"],
        )
        assert result.node_counts == {"airport": 10, "country": 20}
        assert result.edge_counts == {"route": 30, "contains": 40}
        assert result.failed_node_labels == ()
        assert result.failed_edge_labels == ()
        assert result.total_queries == 4

    def test_individual_failures_captured(self):
        mock_client = MagicMock()
        mock_client.execute_cypher.side_effect = [
            {"results": [{"c": 10}]},
            NeptuneClientError("access denied"),
        ]
        result = fetch_label_counts(
            mock_client,
            node_labels=["airport", "country"],
            edge_labels=[],
        )
        assert result.node_counts == {"airport": 10}
        assert result.failed_node_labels == ("country",)


class TestFallbackCLIIntegration:
    def test_fallback_triggers_when_pg_schema_unavailable(self, capsys, pg_summary_json):
        from unittest.mock import patch

        from neptune_schema_stats.cli import main

        pg_schema_err = NeptuneHTTPError(
            NeptuneAPIError(
                code="MalformedQueryException",
                detailed_message="unknown procedure neptune.graph.pg_schema",
            )
        )
        argv = [
            "--endpoint",
            "example.test",
            "--mode",
            "pg",
            "--skip-multi-label-check",
        ]
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=PGSummary.from_json(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                side_effect=pg_schema_err,
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_label_counts",
                return_value=FallbackResult(
                    node_counts={
                        "airport": 3503,
                        "continent": 7,
                        "country": 237,
                        "version": 1,
                    },
                    edge_counts={"route": 50532, "contains": 768},
                ),
            ),
        ):
            code = main(argv)
        assert code == 0
        out = capsys.readouterr().out
        assert "Property Graph Statistics" in out
        assert "airport" in out
        assert "3,503" in out
        assert "route" in out
        assert "50,532" in out
        # Fallback footnote should appear.
        assert "Fallback mode" in out
        assert "1.4.8.0" in out

    def test_api_only_does_not_fall_back(self, capsys, pg_summary_json):
        """--api-only must NOT trigger the fallback queries (respects the
        'no I/O beyond metadata APIs' semantics)."""
        from unittest.mock import patch

        from neptune_schema_stats.cli import main

        pg_schema_err = NeptuneHTTPError(
            NeptuneAPIError(
                code="MalformedQueryException",
                detailed_message="unknown procedure neptune.graph.pg_schema",
            )
        )
        argv = [
            "--endpoint",
            "example.test",
            "--mode",
            "pg",
            "--skip-multi-label-check",
            "--api-only",
        ]
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=PGSummary.from_json(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                side_effect=pg_schema_err,
            ),
            patch("neptune_schema_stats.cli.pg_flow.fetch_label_counts") as fallback_mock,
        ):
            code = main(argv)
        # --api-only propagates the error to top-level; exit 1 (generic runtime error).
        assert code == 1
        fallback_mock.assert_not_called()
        err = capsys.readouterr().err
        # Hint about upgrading engine should appear.
        assert "1.4.8.0" in err
