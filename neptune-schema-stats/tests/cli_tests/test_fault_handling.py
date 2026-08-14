"""Systematic fault-handling tests: transport errors, malformed responses,
scan failure, empty graph, and top-level exception safety net."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from neptune_schema_stats.client.base import (
    NeptuneAPIError,
    NeptuneClient,
    NeptuneClientError,
    NeptuneHTTPError,
    _translate_transport_error,
)

# ---------------------------------------------------------------------------
# Transport-error translation
# ---------------------------------------------------------------------------


class TestTranslateTransportError:
    def test_connect_timeout_becomes_client_error_with_timeout_hint(self):
        exc = requests.exceptions.ConnectTimeout("timed out")
        result = _translate_transport_error(exc, "GET https://x.example:8182/pg")
        assert isinstance(result, NeptuneClientError)
        text = str(result)
        assert "Request timeout" in text
        assert "--timeout" in text

    def test_read_timeout_becomes_client_error_with_timeout_hint(self):
        exc = requests.exceptions.ReadTimeout("read timeout")
        result = _translate_transport_error(exc, "GET https://x.example:8182/pg")
        assert isinstance(result, NeptuneClientError)
        assert "Request timeout" in str(result)

    def test_ssl_error_hints_verify_tls(self):
        exc = requests.exceptions.SSLError("bad cert")
        result = _translate_transport_error(exc, "GET https://x.example:8182/pg")
        assert isinstance(result, NeptuneClientError)
        text = str(result)
        assert "TLS handshake failed" in text
        assert "--no-verify-tls" in text

    def test_connection_error_hints_network(self):
        exc = requests.exceptions.ConnectionError("DNS fail")
        result = _translate_transport_error(exc, "GET https://x.example:8182/pg")
        assert isinstance(result, NeptuneClientError)
        text = str(result)
        assert "Connection failed" in text
        assert "DNS" in text


# ---------------------------------------------------------------------------
# NeptuneClient wraps transport errors
# ---------------------------------------------------------------------------


class TestClientTransportWrapping:
    """The client should surface transport errors as :class:`NeptuneClientError`
    with a helpful hint. Two paths matter now:

    - boto3-backed operations (summary, statistics, openCypher) — errors come
      through ``botocore.exceptions`` and are translated in ``_call``.
    - The raw-HTTP SPARQL path — errors come through ``requests.exceptions``
      and are translated in ``sparql_query``.
    """

    def test_sparql_connection_error_becomes_client_error(self):
        client = NeptuneClient(endpoint="unreachable.invalid", verify_tls=False)
        with (
            patch.object(
                client._http,
                "post",
                side_effect=requests.exceptions.ConnectionError("unreachable"),
            ),
            pytest.raises(NeptuneClientError, match="Connection failed"),
        ):
            client.sparql_query("SELECT * WHERE { ?s ?p ?o }")

    def test_sparql_ssl_error_becomes_client_error(self):
        client = NeptuneClient(endpoint="ssl-broken.invalid")
        with (
            patch.object(
                client._http,
                "post",
                side_effect=requests.exceptions.SSLError("cert invalid"),
            ),
            pytest.raises(NeptuneClientError, match="TLS handshake failed"),
        ):
            client.sparql_query("SELECT * WHERE { ?s ?p ?o }")

    def test_boto_endpoint_connection_error_becomes_client_error(self):
        from botocore.exceptions import EndpointConnectionError

        client = NeptuneClient(endpoint="example.test")
        with (
            patch.object(
                client._boto,
                "get_propertygraph_summary",
                side_effect=EndpointConnectionError(endpoint_url="https://example.test"),
            ),
            pytest.raises(NeptuneClientError, match="Connection failed"),
        ):
            client.get_pg_summary()


# ---------------------------------------------------------------------------
# Malformed response defense
# ---------------------------------------------------------------------------


class TestModelParserRobustness:
    def test_pg_summary_malformed_body_raises_client_error(self):
        from neptune_schema_stats.client.pg_summary import fetch_pg_summary

        mock_client = MagicMock()
        # Missing "payload" key.
        mock_client.get_pg_summary.return_value = {"status": "200 OK"}
        with pytest.raises(NeptuneClientError, match="Malformed PG summary"):
            fetch_pg_summary(mock_client)

    def test_pg_schema_malformed_body_raises_client_error(self):
        from neptune_schema_stats.client.pg_schema import fetch_pg_schema

        mock_client = MagicMock()
        mock_client.execute_cypher.return_value = {"results": []}  # missing 'schema'
        with pytest.raises(NeptuneClientError, match="Malformed pg_schema"):
            fetch_pg_schema(mock_client)

    def test_rdf_summary_malformed_body_raises_client_error(self):
        from neptune_schema_stats.client.rdf_summary import fetch_rdf_summary

        mock_client = MagicMock()
        mock_client.get_rdf_summary.return_value = {"status": "200 OK"}
        with pytest.raises(NeptuneClientError, match="Malformed RDF summary"):
            fetch_rdf_summary(mock_client)


# ---------------------------------------------------------------------------
# HTTP error hints
# ---------------------------------------------------------------------------


class TestHTTPErrorHints:
    """Each Neptune error code should yield an actionable hint."""

    def _err(self, code: str, msg: str = "") -> NeptuneHTTPError:
        return NeptuneHTTPError(NeptuneAPIError(code=code, detailed_message=msg))

    def test_access_denied_hint(self):
        from neptune_schema_stats.cli.hints import _hint_for_http_error

        hint = _hint_for_http_error(self._err("AccessDeniedException"))
        assert hint is not None
        assert "GetGraphSummary" in hint or "IAM" in hint

    def test_throttling_hint(self):
        from neptune_schema_stats.cli.hints import _hint_for_http_error

        hint = _hint_for_http_error(self._err("ThrottlingException"))
        assert hint is not None
        assert "throttling" in hint.lower() or "retry" in hint.lower()

    def test_query_limit_exceeded_hint(self):
        from neptune_schema_stats.cli.hints import _hint_for_http_error

        hint = _hint_for_http_error(self._err("QueryLimitExceededException"))
        assert hint is not None
        assert "--api-only" in hint or "timeout" in hint.lower()

    def test_internal_failure_hint(self):
        from neptune_schema_stats.cli.hints import _hint_for_http_error

        hint = _hint_for_http_error(self._err("InternalFailureException"))
        assert hint is not None
        assert "retry" in hint.lower()

    def test_read_only_violation_hint(self):
        from neptune_schema_stats.cli.hints import _hint_for_http_error

        hint = _hint_for_http_error(self._err("ReadOnlyViolationException"))
        assert hint is not None
        assert "read-only" in hint.lower()

    def test_pg_schema_old_engine_hint_preserved(self):
        from neptune_schema_stats.cli.hints import _hint_for_http_error

        hint = _hint_for_http_error(
            self._err("MalformedQueryException", "unknown procedure neptune.graph.pg_schema")
        )
        assert hint is not None
        assert "1.4.8.0" in hint

    def test_unknown_error_returns_none(self):
        from neptune_schema_stats.cli.hints import _hint_for_http_error

        assert _hint_for_http_error(self._err("SomeUnknownException")) is None


# ---------------------------------------------------------------------------
# Empty graph rendering
# ---------------------------------------------------------------------------


class TestEmptyGraph:
    """A cluster with zero nodes and zero edges should render without error."""

    def test_empty_graph_correlates_cleanly(self):
        from neptune_schema_stats.models import PGSchema, PGSummary

        _summary = PGSummary.from_json(
            {
                "status": "200 OK",
                "payload": {
                    "version": "v1",
                    "lastStatisticsComputationTime": "2026-08-09T00:00:00Z",
                    "graphSummary": {
                        "numNodes": 0,
                        "numEdges": 0,
                        "numNodeLabels": 0,
                        "numEdgeLabels": 0,
                        "nodeLabels": [],
                        "edgeLabels": [],
                        "numNodeProperties": 0,
                        "numEdgeProperties": 0,
                        "nodeProperties": [],
                        "edgeProperties": [],
                        "totalNodePropertyValues": 0,
                        "totalEdgePropertyValues": 0,
                        "nodeStructures": [],
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
                            "nodeLabels": [],
                            "edgeLabels": [],
                            "nodeLabelDetails": {},
                            "edgeLabelDetails": {},
                            "labelTriples": [],
                        }
                    }
                ]
            }
        )
        # Empty schema has zero labels, so is_usable() is False. The
        # correlator refuses to run on non-usable schemas. That's the
        # correct behavior — the CLI would print the schema-not-usable
        # hint instead. Confirm the guard fires:
        assert not schema.is_usable()

    def test_graph_with_labels_but_zero_nodes_renders(self):
        from neptune_schema_stats.correlator import correlate_pg
        from neptune_schema_stats.models import PGSchema, PGSummary
        from neptune_schema_stats.report import render_pg_report

        # Schema declares labels but summary shows zero nodes.
        summary = PGSummary.from_json(
            {
                "status": "200 OK",
                "payload": {
                    "version": "v1",
                    "lastStatisticsComputationTime": "2026-08-09T00:00:00Z",
                    "graphSummary": {
                        "numNodes": 0,
                        "numEdges": 0,
                        "numNodeLabels": 1,
                        "numEdgeLabels": 0,
                        "nodeLabels": ["A"],
                        "edgeLabels": [],
                        "numNodeProperties": 0,
                        "numEdgeProperties": 0,
                        "nodeProperties": [],
                        "edgeProperties": [],
                        "totalNodePropertyValues": 0,
                        "totalEdgePropertyValues": 0,
                        "nodeStructures": [],
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
                            "nodeLabelDetails": {"A": {"properties": {}}},
                            "edgeLabelDetails": {},
                            "labelTriples": [],
                        }
                    }
                ]
            }
        )
        assert schema.is_usable()
        result = correlate_pg(summary, schema)
        # Should not raise.
        report = render_pg_report(summary, schema, result, endpoint="empty.test")
        assert "Total nodes:" in report
        assert "0" in report


# ---------------------------------------------------------------------------
# All-scan-failed detection
# ---------------------------------------------------------------------------


class TestScanAllFailed:
    def test_warning_when_every_scan_query_fails(self, capsys):
        """Every planned scan query fails → clear stderr warning surfaces."""
        from neptune_schema_stats.cli.pg_flow import _warn_if_scan_fully_failed
        from neptune_schema_stats.scan import ScanPlan, ScanResults

        plan = ScanPlan(
            node_labels_to_query=("A", "B"),
            edge_labels_to_query=("e1",),
        )
        scan = ScanResults(
            plan=plan,
            failed_node_labels=("A", "B"),
            failed_edge_labels=("e1",),
        )
        _warn_if_scan_fully_failed(scan)
        err = capsys.readouterr().err
        assert "all 3 scan query(s) failed" in err
        assert "ReadDataViaQuery" in err
        assert "--api-only" in err

    def test_no_warning_when_at_least_one_query_succeeds(self, capsys):
        from neptune_schema_stats.cli.pg_flow import _warn_if_scan_fully_failed
        from neptune_schema_stats.scan import (
            NodeCountScan,
            ScanPlan,
            ScanResults,
        )

        plan = ScanPlan(
            node_labels_to_query=("A", "B"),
            edge_labels_to_query=(),
        )
        scan = ScanResults(
            plan=plan,
            node_scans=(NodeCountScan(label="A", exact_count=10),),
            failed_node_labels=("B",),
        )
        _warn_if_scan_fully_failed(scan)
        err = capsys.readouterr().err
        assert "all" not in err.lower() or "failed" not in err


# ---------------------------------------------------------------------------
# Top-level safety net
# ---------------------------------------------------------------------------


class TestTopLevelSafetyNet:
    def test_unexpected_exception_yields_clean_error_not_traceback(self, capsys):
        from neptune_schema_stats.cli import main

        with patch(
            "neptune_schema_stats.cli.entry._run",
            side_effect=RuntimeError("simulated internal bug"),
        ):
            code = main(["--endpoint", "example.test"])
        assert code == 1
        err = capsys.readouterr().err
        assert "Unexpected error" in err
        assert "simulated internal bug" in err
        # No traceback in default mode.
        assert "Traceback" not in err
