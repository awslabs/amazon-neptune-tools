"""Tests for the Neptune client and mode auto-detection.

The client is now a thin wrapper around ``boto3.client('neptunedata')``
plus a raw-HTTP SPARQL path. Tests mock at the boto3 client level
(``client._boto``) or at the high-level API methods (``get_pg_summary``,
``execute_cypher``, ``sparql_query``).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from neptune_schema_stats.client.base import (
    IAMAuthConfig,
    NeptuneClient,
    NeptuneClientError,
    NeptuneHTTPError,
    NeptuneStatisticsNotAvailableError,
)
from neptune_schema_stats.client.pg_schema import fetch_pg_schema
from neptune_schema_stats.client.pg_summary import fetch_pg_summary
from neptune_schema_stats.client.rdf_summary import fetch_rdf_summary
from neptune_schema_stats.detect import ModeDetectionError, detect_mode, probe_endpoints
from neptune_schema_stats.models import GraphMode, SchemaState


def _mock_response(
    body: dict[str, Any] | str,
    *,
    status: int = 200,
    ok: bool | None = None,
) -> MagicMock:
    """Build a fake ``requests.Response`` for the SPARQL raw-HTTP path."""
    response = MagicMock()
    response.status_code = status
    response.ok = (status < 400) if ok is None else ok
    response.url = "https://example.test/mock"
    if isinstance(body, dict):
        response.json.return_value = body
        response.text = json.dumps(body)
    else:
        response.json.side_effect = json.JSONDecodeError("expected value", body, 0)
        response.text = body
    return response


class TestBaseUrlBuilding:
    def test_bare_host_with_default_port(self):
        c = NeptuneClient("cluster.neptune.amazonaws.com")
        assert c.base_url == "https://cluster.neptune.amazonaws.com:8182"

    def test_bare_host_with_explicit_port(self):
        c = NeptuneClient("cluster.neptune.amazonaws.com", port=443)
        assert c.base_url == "https://cluster.neptune.amazonaws.com:443"

    def test_full_url_is_preserved(self):
        c = NeptuneClient("https://cluster.neptune.amazonaws.com:8182")
        assert c.base_url == "https://cluster.neptune.amazonaws.com:8182"

    def test_host_with_port_in_endpoint(self):
        c = NeptuneClient("cluster.neptune.amazonaws.com:9000")
        assert c.base_url == "https://cluster.neptune.amazonaws.com:9000"

    def test_strips_trailing_slash(self):
        c = NeptuneClient("https://cluster.example/")
        assert c.base_url == "https://cluster.example"


class TestBotoClientConstruction:
    def test_no_iam_uses_unsigned(self):
        c = NeptuneClient("example.test")
        # When no IAM config is provided, we configure boto3 with UNSIGNED
        # so calls go through without credential lookup.
        from botocore import UNSIGNED

        assert c._boto.meta.config.signature_version is UNSIGNED

    def test_iam_uses_signed_and_region(self):
        # Prevent boto3 from actually resolving credentials against the env
        # by returning a fake session that has credentials.
        with patch("neptune_schema_stats.client.base.boto3.Session") as mock_session_cls:
            fake_session = MagicMock()
            fake_session.get_credentials.return_value = MagicMock()
            fake_session.client.return_value = MagicMock()
            mock_session_cls.return_value = fake_session
            NeptuneClient("example.test", iam=IAMAuthConfig(region="us-west-2"))
        assert mock_session_cls.called
        # region_name reached the client factory
        _, kwargs = fake_session.client.call_args
        assert kwargs["region_name"] == "us-west-2"

    def test_iam_without_credentials_raises(self):
        with patch("neptune_schema_stats.client.base.boto3.Session") as mock_session_cls:
            fake_session = MagicMock()
            fake_session.get_credentials.return_value = None
            mock_session_cls.return_value = fake_session
            with pytest.raises(NeptuneClientError, match="no AWS credentials"):
                NeptuneClient("example.test", iam=IAMAuthConfig(region="us-east-1"))


class TestBotoErrorTranslation:
    """Boto errors are translated into our typed exception hierarchy."""

    def test_client_error_becomes_neptune_http_error(self):
        c = NeptuneClient("example.test")
        client_err = ClientError(
            error_response={
                "Error": {"Code": "AccessDeniedException", "Message": "Nope"},
                "ResponseMetadata": {"HTTPStatusCode": 403, "RequestId": "req-1"},
            },
            operation_name="GetPropertygraphSummary",
        )
        c._boto = MagicMock()
        c._boto.get_propertygraph_summary.side_effect = client_err
        with pytest.raises(NeptuneHTTPError) as excinfo:
            c.get_pg_summary()
        assert excinfo.value.error.code == "AccessDeniedException"
        assert excinfo.value.error.http_status == 403

    def test_statistics_not_available_is_distinct_error(self):
        c = NeptuneClient("example.test")
        client_err = ClientError(
            error_response={
                "Error": {"Code": "StatisticsNotAvailableException", "Message": ""},
                "ResponseMetadata": {"HTTPStatusCode": 400},
            },
            operation_name="GetPropertygraphSummary",
        )
        c._boto = MagicMock()
        c._boto.get_propertygraph_summary.side_effect = client_err
        with pytest.raises(NeptuneStatisticsNotAvailableError):
            c.get_pg_summary()


class TestSPARQLQuery:
    """The one operation neptunedata doesn't cover — arbitrary SPARQL still
    goes through raw HTTP with our own SigV4 signing."""

    def test_sends_form_encoded_body(self):
        client = NeptuneClient("example.test")
        with patch.object(client._http, "post") as mock_post:
            mock_post.return_value = _mock_response(
                {"head": {"vars": ["c"]}, "results": {"bindings": []}}
            )
            client.sparql_query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }")
        kwargs = mock_post.call_args.kwargs
        assert kwargs["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
        assert b"query=" in kwargs["data"]

    def test_no_iam_no_auth_header(self):
        client = NeptuneClient("example.test", iam=None)
        with patch.object(client._http, "post") as mock_post:
            mock_post.return_value = _mock_response(
                {"head": {"vars": ["c"]}, "results": {"bindings": []}}
            )
            client.sparql_query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }")
        headers = mock_post.call_args.kwargs["headers"]
        assert "Authorization" not in headers

    def test_iam_adds_sigv4_authorization(self):
        iam = IAMAuthConfig(region="us-east-1")
        # Fake credentials get resolved once at NeptuneClient construction
        # and cached; SPARQL signing reuses them without re-walking the chain.
        fake_creds = MagicMock()
        fake_creds.access_key = "AKIATEST"
        fake_creds.secret_key = "secret"
        fake_creds.token = None
        with patch("neptune_schema_stats.client.base.boto3.Session") as mock_session_cls:
            fake_session = MagicMock()
            fake_session.get_credentials.return_value.get_frozen_credentials.return_value = (
                fake_creds
            )
            fake_session.client.return_value = MagicMock()
            mock_session_cls.return_value = fake_session
            client = NeptuneClient("example.test", iam=iam)

        with patch.object(client._http, "post") as mock_post:
            mock_post.return_value = _mock_response(
                {"head": {"vars": []}, "results": {"bindings": []}}
            )
            client.sparql_query("SELECT ?s WHERE { ?s ?p ?o }")

        headers = mock_post.call_args.kwargs["headers"]
        assert "Authorization" in headers
        assert headers["Authorization"].startswith("AWS4-HMAC-SHA256")
        assert "X-Amz-Date" in headers

    def test_error_response_becomes_http_error(self):
        client = NeptuneClient("example.test")
        error_body = {"code": "AccessDeniedException", "detailedMessage": "Nope"}
        with patch.object(client._http, "post") as mock_post:
            mock_post.return_value = _mock_response(error_body, status=403)
            with pytest.raises(NeptuneHTTPError) as excinfo:
                client.sparql_query("SELECT ?s WHERE { ?s ?p ?o }")
        assert excinfo.value.error.code == "AccessDeniedException"

    def test_non_json_response_becomes_client_error(self):
        client = NeptuneClient("example.test")
        with patch.object(client._http, "post") as mock_post:
            mock_post.return_value = _mock_response("<html>oops</html>", status=502)
            with pytest.raises(NeptuneClientError):
                client.sparql_query("SELECT ?s WHERE { ?s ?p ?o }")


class TestAPIClients:
    def test_fetch_pg_summary_returns_parsed_model(self, pg_summary_json):
        client = NeptuneClient("example.test")
        with patch.object(client, "get_pg_summary", return_value=pg_summary_json):
            summary = fetch_pg_summary(client)
        assert summary.num_nodes == 3748
        assert len(summary.node_structures) == 5

    def test_fetch_pg_schema_returns_parsed_model(self, pg_schema_json):
        client = NeptuneClient("example.test")
        with patch.object(client, "execute_cypher", return_value=pg_schema_json):
            schema = fetch_pg_schema(client)
        assert schema.state() == SchemaState.COMPLETED
        assert "airport" in schema.node_label_details

    def test_fetch_rdf_summary_returns_parsed_model(self, rdf_summary_json):
        client = NeptuneClient("example.test")
        with patch.object(client, "get_rdf_summary", return_value=rdf_summary_json):
            summary = fetch_rdf_summary(client)
        assert summary.num_distinct_subjects == 54403
        assert summary.num_classes == 4


class TestModeDetection:
    def test_prefers_pg_when_both_populated(self, pg_summary_json, rdf_summary_json):
        client = NeptuneClient("example.test")
        with (
            patch(
                "neptune_schema_stats.detect.fetch_pg_summary",
                return_value=type("PG", (), {"num_nodes": 3748})(),
            ),
            patch(
                "neptune_schema_stats.detect.fetch_rdf_summary",
                return_value=type("RDF", (), {"num_quads": 158571})(),
            ),
        ):
            assert detect_mode(client) is GraphMode.PG

    def test_falls_back_to_rdf_when_pg_empty(self):
        client = NeptuneClient("example.test")
        empty_pg = type("PG", (), {"num_nodes": 0})()
        populated_rdf = type("RDF", (), {"num_quads": 158571})()
        with (
            patch("neptune_schema_stats.detect.fetch_pg_summary", return_value=empty_pg),
            patch("neptune_schema_stats.detect.fetch_rdf_summary", return_value=populated_rdf),
        ):
            assert detect_mode(client) is GraphMode.RDF

    def test_stats_not_available_still_indicates_endpoint_presence(self):
        """StatisticsNotAvailableException means the endpoint exists — try RDF but return PG."""
        from neptune_schema_stats.models import NeptuneAPIError

        client = NeptuneClient("example.test")
        err = NeptuneStatisticsNotAvailableError(
            NeptuneAPIError(code="StatisticsNotAvailableException", detailed_message="")
        )
        populated_rdf = type("RDF", (), {"num_quads": 158571})()
        with (
            patch("neptune_schema_stats.detect.fetch_pg_summary", side_effect=err),
            patch("neptune_schema_stats.detect.fetch_rdf_summary", return_value=populated_rdf),
        ):
            result = probe_endpoints(client)
            assert result.pg_summary is None
            assert result.pg_error is None
            assert result.rdf_summary is not None
            assert detect_mode(client) is GraphMode.RDF

    def test_raises_when_both_fail(self):
        client = NeptuneClient("example.test")
        with (
            patch(
                "neptune_schema_stats.detect.fetch_pg_summary",
                side_effect=NeptuneClientError("timeout"),
            ),
            patch(
                "neptune_schema_stats.detect.fetch_rdf_summary",
                side_effect=NeptuneClientError("timeout"),
            ),
            pytest.raises(ModeDetectionError),
        ):
            detect_mode(client)
