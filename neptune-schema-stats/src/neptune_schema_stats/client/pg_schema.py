"""Client for the PG Schema API (openCypher ``neptune.graph.pg_schema``).

.. note::
    The ``neptune.graph.pg_schema`` procedure is only available on Neptune engine
    versions **1.4.8.0 or later**. Older clusters will reject the query with a
    ``MalformedQueryException`` (unknown procedure). We surface that as a
    :class:`NeptuneHTTPError` and let the caller decide how to present it.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from neptune_schema_stats.client.base import NeptuneClient
from neptune_schema_stats.client.opencypher import execute_cypher
from neptune_schema_stats.models import PGSchema, SchemaState

PG_SCHEMA_QUERY = "CALL neptune.graph.pg_schema()"
PG_SCHEMA_COMPUTE_QUERY = "CALL neptune.graph.pg_schema.compute()"

TERMINAL_STATES: frozenset[SchemaState] = frozenset(
    {SchemaState.COMPLETED, SchemaState.FAILED, SchemaState.STOPPED}
)

# Default polling behavior for wait_for_schema.
DEFAULT_POLL_INTERVAL_SECONDS = 10.0
DEFAULT_POLL_TIMEOUT_SECONDS = 600.0


def fetch_pg_schema(client: NeptuneClient) -> PGSchema:
    """Retrieve the current PG schema (property-graph metadata).

    Returns a :class:`PGSchema` whose ``state()`` may be ``NotStarted``,
    ``InProgress``, ``Completed``, ``Failed``, or ``Stopped``.
    Only ``Completed`` schemas contain populated labels and label triples.
    """
    body = execute_cypher(client, PG_SCHEMA_QUERY)
    try:
        return PGSchema.from_json(body)
    except (KeyError, TypeError, ValueError) as exc:
        from neptune_schema_stats.client.base import NeptuneClientError

        raise NeptuneClientError(f"Malformed pg_schema response: {exc}") from exc


def trigger_pg_schema_compute(client: NeptuneClient) -> dict[str, Any]:
    """Kick off asynchronous PG schema computation.

    Returns the raw openCypher response. The compute runs in the background;
    call :func:`fetch_pg_schema` or :func:`wait_for_schema` to observe progress.
    """
    return execute_cypher(client, PG_SCHEMA_COMPUTE_QUERY)


def wait_for_schema(
    client: NeptuneClient,
    *,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout: float = DEFAULT_POLL_TIMEOUT_SECONDS,
    on_poll: Callable[[PGSchema], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> PGSchema:
    """Poll ``fetch_pg_schema`` until the state is terminal or the timeout elapses.

    Terminal states are ``Completed``, ``Failed``, and ``Stopped``.

    :param poll_interval: Seconds to wait between polls.
    :param timeout: Maximum seconds to wait before raising :class:`TimeoutError`.
    :param on_poll: Optional callback invoked with each :class:`PGSchema` polled;
        useful for progress logging.
    :param sleep: Injected for tests. Defaults to :func:`time.sleep`.
    :param monotonic: Injected for tests. Defaults to :func:`time.monotonic`.
    :raises TimeoutError: If a terminal state is not reached before ``timeout`` seconds.
    """
    deadline = monotonic() + timeout
    while True:
        schema = fetch_pg_schema(client)
        if on_poll is not None:
            on_poll(schema)
        if schema.state() in TERMINAL_STATES:
            return schema
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"pg_schema did not reach a terminal state within {timeout:.0f}s; "
                f"last state was {schema.state().value} "
                f"({schema.status.progress_percentage}%)."
            )
        sleep(min(poll_interval, remaining))
