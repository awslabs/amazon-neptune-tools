"""Neptune data-plane client — boto3 ``neptunedata`` for most operations,
with a raw-HTTP fallback for arbitrary SPARQL (which the SDK does not expose).

The boto3 client handles:

- ``GetPropertygraphSummary`` / ``GetRDFGraphSummary``
- ``GetPropertygraphStatistics`` / ``ManagePropertygraphStatistics``
- ``GetSparqlStatistics`` / ``ManageSparqlStatistics``
- ``ExecuteOpenCypherQuery``

SigV4 signing, retries, and error parsing are automatic. Set
``iam=None`` and the client uses :data:`botocore.UNSIGNED` so it can talk
to Neptune clusters that have IAM auth disabled.

Arbitrary SPARQL queries still go through :meth:`NeptuneClient.sparql_query`,
which does raw HTTP to ``/sparql`` (SigV4-signed when ``iam`` is set,
reusing the credentials already resolved for the boto3 client).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import boto3
import requests
from botocore import UNSIGNED
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import ReadOnlyCredentials
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from botocore.exceptions import (
    SSLError as BotoSSLError,
)

from neptune_schema_stats.models import NeptuneAPIError

log = logging.getLogger(__name__)

NEPTUNE_SERVICE_NAME = "neptune-db"
DEFAULT_TIMEOUT_SECONDS = 30.0


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class NeptuneClientError(Exception):
    """Base class for all Neptune client errors."""


class NeptuneHTTPError(NeptuneClientError):
    """A structured Neptune API error was returned (with code + detailedMessage)."""

    def __init__(self, error: NeptuneAPIError) -> None:
        super().__init__(str(error))
        self.error = error


class NeptuneStatisticsNotAvailableError(NeptuneHTTPError):
    """The Graph Summary API returned StatisticsNotAvailableException.

    This is a distinct exception so callers (like mode auto-detection) can
    treat it specifically — the endpoint exists but stats haven't been computed.
    """


# ---------------------------------------------------------------------------
# Auth config
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class IAMAuthConfig:
    """Configuration for AWS SigV4 signing of Neptune requests."""

    region: str
    profile: str | None = None
    service: str = NEPTUNE_SERVICE_NAME


# ---------------------------------------------------------------------------
# Error translation
# ---------------------------------------------------------------------------

# Transport-layer errors can come from two places: the boto3 client (using
# ``botocore.exceptions``) and the raw-HTTP SPARQL path (using
# ``requests.exceptions``). The two libraries name their exceptions
# differently but the categories map cleanly.

_TIMEOUT_EXCEPTIONS: tuple[type[Exception], ...] = (
    ConnectTimeoutError,
    ReadTimeoutError,
    requests.exceptions.ConnectTimeout,
    requests.exceptions.ReadTimeout,
    requests.exceptions.Timeout,
)

_SSL_EXCEPTIONS: tuple[type[Exception], ...] = (
    BotoSSLError,
    requests.exceptions.SSLError,
)

_CONNECTION_EXCEPTIONS: tuple[type[Exception], ...] = (
    EndpointConnectionError,
    requests.exceptions.ConnectionError,
)

_HINTS: dict[str, tuple[str, str]] = {
    "timeout": (
        "Request timeout",
        "the request took longer than the client timeout. Raise --timeout, "
        "wait for slow queries to finish, or check cluster health.",
    ),
    "ssl": (
        "TLS handshake failed",
        "the endpoint's certificate could not be verified. Confirm the "
        "endpoint hostname, check any MITM proxy in the path, or if this "
        "is a dev/test cluster you can pass --no-verify-tls.",
    ),
    "connection": (
        "Connection failed",
        "the endpoint could not be reached. Check the host is correct, DNS "
        "resolves, your network path (VPC/bastion/port forward) is up, and "
        "that port 8182 is open.",
    ),
}


def _translate_transport_error(exc: Exception, context: str) -> NeptuneClientError:
    """Translate a transport-layer exception (from botocore *or* requests) into
    a :class:`NeptuneClientError` carrying a user-facing hint.

    ``context`` describes what was being attempted — e.g. the boto3 operation
    name (``"get_propertygraph_summary"``) or the raw HTTP call
    (``"POST https://host:8182/sparql"``). Included verbatim in the message.
    """
    if isinstance(exc, _TIMEOUT_EXCEPTIONS):
        kind, hint = _HINTS["timeout"]
    elif isinstance(exc, _SSL_EXCEPTIONS):
        kind, hint = _HINTS["ssl"]
    elif isinstance(exc, _CONNECTION_EXCEPTIONS):
        kind, hint = _HINTS["connection"]
    else:
        kind = "Transport error"
        hint = f"{type(exc).__name__} — retry or check network path."
    return NeptuneClientError(f"{kind} in {context}: {exc}\n  Hint: {hint}")


def _translate_boto_client_error(exc: ClientError) -> NeptuneHTTPError:
    """Convert a ``botocore.ClientError`` into a typed :class:`NeptuneHTTPError`.

    Preserves the Neptune error code, message, and request ID so downstream
    code (like ``is_statistics_unavailable_error`` or the CLI hint layer)
    keeps working unchanged.
    """
    err = exc.response.get("Error", {}) or {}
    meta = exc.response.get("ResponseMetadata", {}) or {}
    code = str(err.get("Code", "Unknown"))
    detailed = str(err.get("Message", ""))
    request_id = str(meta.get("RequestId", ""))
    http_status = int(meta.get("HTTPStatusCode", 0) or 0)
    api_err = NeptuneAPIError(
        code=code,
        detailed_message=detailed,
        request_id=request_id,
        http_status=http_status,
        raw=exc.response,
    )
    if code == "StatisticsNotAvailableException":
        return NeptuneStatisticsNotAvailableError(api_err)
    return NeptuneHTTPError(api_err)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class NeptuneClient:
    """Neptune data-plane client.

    Wraps a ``boto3.client('neptunedata')`` for the operations the SDK
    supports, plus a raw ``requests`` session for arbitrary SPARQL. Not
    thread-safe; instantiate once per invocation.
    """

    def __init__(
        self,
        endpoint: str,
        port: int = 8182,
        *,
        iam: IAMAuthConfig | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        verify_tls: bool = True,
    ) -> None:
        self._base_url = self._build_base_url(endpoint, port)
        self._iam = iam
        self._timeout = timeout
        self._verify_tls = verify_tls
        # Cached credentials for SPARQL SigV4 signing. Resolved once at
        # construction rather than per-call. ``None`` under UNSIGNED mode.
        self._credentials: ReadOnlyCredentials | None = None
        self._boto = self._build_boto_client()
        self._http = requests.Session()

    @staticmethod
    def _build_base_url(endpoint: str, port: int) -> str:
        """Normalize an endpoint into a base URL. Neptune only supports HTTPS."""
        endpoint = endpoint.strip().rstrip("/")
        if endpoint.startswith(("http://", "https://")):
            return endpoint
        if ":" in endpoint and not endpoint.endswith("]"):  # naive IPv6 skip
            return f"https://{endpoint}"
        return f"https://{endpoint}:{port}"

    @property
    def base_url(self) -> str:
        return self._base_url

    # ------------------------------------------------------------------
    # Boto3 factory
    # ------------------------------------------------------------------

    def _build_boto_client(self) -> Any:
        """Build the underlying boto3 ``neptunedata`` client.

        When ``self._iam`` is ``None`` we use :data:`botocore.UNSIGNED` so
        the client can talk to Neptune clusters with IAM auth disabled
        (typical for dev/test).
        """
        config_kwargs: dict[str, Any] = {
            "read_timeout": self._timeout,
            "connect_timeout": self._timeout,
            # One-shot CLI — retries just delay error surfacing. Users can rerun.
            "retries": {"max_attempts": 1},
        }
        if self._iam is None:
            config = Config(signature_version=UNSIGNED, **config_kwargs)
            # region is required by boto3 but ignored under UNSIGNED.
            return boto3.client(
                "neptunedata",
                endpoint_url=self._base_url,
                region_name="us-east-1",
                config=config,
                verify=self._verify_tls,
            )
        session = (
            boto3.Session(profile_name=self._iam.profile) if self._iam.profile else boto3.Session()
        )
        creds = session.get_credentials()
        if creds is None:
            raise NeptuneClientError(
                "IAM auth requested but no AWS credentials were found. "
                "Configure credentials via environment variables, "
                "~/.aws/credentials, or an instance role, and retry."
            )
        # Cache frozen credentials for reuse in SPARQL signing.
        self._credentials = creds.get_frozen_credentials()
        return session.client(
            "neptunedata",
            endpoint_url=self._base_url,
            region_name=self._iam.region,
            config=Config(**config_kwargs),
            verify=self._verify_tls,
        )

    # ------------------------------------------------------------------
    # Neptune data-plane operations (via boto3 neptunedata)
    # ------------------------------------------------------------------

    def get_pg_summary(self) -> dict[str, Any]:
        """Fetch the property-graph summary (detailed mode)."""
        return self._call("get_propertygraph_summary", mode="detailed")

    def get_rdf_summary(self) -> dict[str, Any]:
        """Fetch the RDF summary (detailed mode)."""
        return self._call("get_rdf_graph_summary", mode="detailed")

    def get_pg_statistics(self) -> dict[str, Any]:
        """Fetch the DFE statistics status for property-graph data."""
        return self._call("get_propertygraph_statistics")

    def refresh_pg_statistics(self) -> dict[str, Any]:
        """Trigger a manual PG statistics refresh."""
        return self._call("manage_propertygraph_statistics", mode="refresh")

    def get_rdf_statistics(self) -> dict[str, Any]:
        """Fetch the DFE statistics status for RDF data."""
        return self._call("get_sparql_statistics")

    def refresh_rdf_statistics(self) -> dict[str, Any]:
        """Trigger a manual RDF statistics refresh."""
        return self._call("manage_sparql_statistics", mode="refresh")

    def execute_cypher(self, query: str) -> dict[str, Any]:
        """Execute an openCypher query."""
        return self._call("execute_open_cypher_query", openCypherQuery=query)

    # ------------------------------------------------------------------
    # SPARQL (raw HTTP — neptunedata does not expose this)
    # ------------------------------------------------------------------

    def sparql_query(self, query: str) -> dict[str, Any]:
        """Execute a SPARQL query. Returns the parsed JSON body.

        Uses raw HTTP because ``boto3.client('neptunedata')`` does not
        expose an arbitrary-SPARQL operation.
        """
        url = f"{self._base_url}/sparql"
        body = urlencode({"query": query}).encode("utf-8")
        headers = self._sparql_headers(url, body=body)
        log.debug("POST %s (sparql)", url)
        try:
            response = self._http.post(
                url,
                data=body,
                headers=headers,
                timeout=self._timeout,
                verify=self._verify_tls,
            )
        except requests.exceptions.RequestException as exc:
            raise _translate_transport_error(exc, f"POST {url}") from exc
        return _parse_sparql_response(response)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _call(self, op_name: str, **kwargs: Any) -> dict[str, Any]:
        """Invoke a boto3 neptunedata operation, translating errors."""
        op = getattr(self._boto, op_name)
        try:
            return op(**kwargs)
        except ClientError as exc:
            raise _translate_boto_client_error(exc) from exc
        except (
            EndpointConnectionError,
            ConnectTimeoutError,
            ReadTimeoutError,
            BotoSSLError,
        ) as exc:
            raise _translate_transport_error(exc, op_name) from exc

    def _sparql_headers(self, url: str, *, body: bytes) -> dict[str, str]:
        """Return headers for the SPARQL request, SigV4-signed when IAM
        auth is configured. Reuses the credentials resolved at construction."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        if self._iam is None or self._credentials is None:
            return headers
        aws_request = AWSRequest(method="POST", url=url, data=body, headers=headers)
        SigV4Auth(self._credentials, self._iam.service, self._iam.region).add_auth(aws_request)
        return dict(aws_request.headers.items())


# ---------------------------------------------------------------------------
# SPARQL response parsing
# ---------------------------------------------------------------------------


def _parse_sparql_response(response: requests.Response) -> dict[str, Any]:
    """Parse a SPARQL JSON response body, raising typed exceptions on errors.

    SPARQL responses never carry ``StatisticsNotAvailableException`` — that
    error only comes from the Graph Summary API. We therefore only need
    the generic error paths here.
    """
    try:
        body = response.json()
    except json.JSONDecodeError as e:
        raise NeptuneClientError(
            f"Non-JSON SPARQL response from {response.url} "
            f"(HTTP {response.status_code}): {response.text[:200]!r}"
        ) from e

    if not response.ok:
        raise NeptuneHTTPError(NeptuneAPIError.from_json(body, http_status=response.status_code))

    if not isinstance(body, dict):
        raise NeptuneClientError(
            f"Unexpected top-level JSON shape (expected object, got {type(body).__name__})"
        )
    return body
