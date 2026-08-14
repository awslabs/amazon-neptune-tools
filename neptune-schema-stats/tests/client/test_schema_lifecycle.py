"""Tests for the PG schema lifecycle helpers.

Covers ``trigger_pg_schema_compute`` and ``wait_for_schema`` with injected
time functions so tests are deterministic and fast.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from neptune_schema_stats.client.base import NeptuneClient
from neptune_schema_stats.client.pg_schema import (
    PG_SCHEMA_COMPUTE_QUERY,
    trigger_pg_schema_compute,
    wait_for_schema,
)
from neptune_schema_stats.models import PGSchema, SchemaState


class TestTriggerPGSchemaCompute:
    def test_posts_compute_query_and_returns_response(self):
        client = NeptuneClient("example.test")
        with patch.object(client, "execute_cypher", return_value={"results": []}) as mock_post:
            body = trigger_pg_schema_compute(client)
        assert body == {"results": []}
        args, _ = mock_post.call_args
        assert args[0] == PG_SCHEMA_COMPUTE_QUERY


class TestWaitForSchema:
    def test_returns_immediately_when_already_completed(self, pg_schema_json):
        completed = PGSchema.from_json(pg_schema_json)
        client = NeptuneClient("example.test")
        sleep_calls: list[float] = []
        with patch(
            "neptune_schema_stats.client.pg_schema.fetch_pg_schema",
            return_value=completed,
        ) as mock_fetch:
            result = wait_for_schema(
                client,
                poll_interval=1.0,
                timeout=10.0,
                sleep=sleep_calls.append,
            )
        assert result is completed
        assert mock_fetch.call_count == 1
        assert sleep_calls == []  # Never slept — first poll was terminal.

    def test_polls_until_terminal_state(
        self, pg_schema_not_started_json, pg_schema_in_progress_json, pg_schema_json
    ):
        not_started = PGSchema.from_json(pg_schema_not_started_json)
        in_progress = PGSchema.from_json(pg_schema_in_progress_json)
        completed = PGSchema.from_json(pg_schema_json)
        client = NeptuneClient("example.test")

        sleep_calls: list[float] = []
        # Fake monotonic clock advances by 1s each call.
        clock = iter(float(i) for i in range(100))

        polled: list[PGSchema] = []
        with patch(
            "neptune_schema_stats.client.pg_schema.fetch_pg_schema",
            side_effect=[not_started, in_progress, completed],
        ) as mock_fetch:
            result = wait_for_schema(
                client,
                poll_interval=5.0,
                timeout=60.0,
                on_poll=polled.append,
                sleep=sleep_calls.append,
                monotonic=lambda: next(clock),
            )

        assert result is completed
        assert mock_fetch.call_count == 3
        assert [s.state() for s in polled] == [
            SchemaState.NOT_STARTED,
            SchemaState.IN_PROGRESS,
            SchemaState.COMPLETED,
        ]
        # Slept twice — once after each non-terminal poll.
        assert len(sleep_calls) == 2

    def test_raises_timeout_when_never_reaches_terminal(self, pg_schema_in_progress_json):
        in_progress = PGSchema.from_json(pg_schema_in_progress_json)
        client = NeptuneClient("example.test")

        # Monotonic returns t=0, then t=100 (>= deadline).
        clock = iter([0.0, 100.0])
        with (
            patch(
                "neptune_schema_stats.client.pg_schema.fetch_pg_schema",
                return_value=in_progress,
            ),
            pytest.raises(TimeoutError) as excinfo,
        ):
            wait_for_schema(
                client,
                poll_interval=10.0,
                timeout=30.0,
                sleep=lambda _s: None,
                monotonic=lambda: next(clock),
            )
        assert "InProgress" in str(excinfo.value)
        assert "42%" in str(excinfo.value)

    def test_returns_failed_state_without_raising(self, pg_schema_not_started_json):
        # Synthesize a Failed schema by mutating the fixture.
        failed_payload = dict(pg_schema_not_started_json)
        failed_payload["results"] = [
            {
                "schema": {
                    "status": {
                        "state": "Failed",
                        "concurrency": "16",
                        "lastComputedTimestamp": "",
                        "progressPercentage": "0",
                        "errorMessage": "insufficient memory",
                    },
                    "nodeLabels": [],
                    "edgeLabels": [],
                    "nodeLabelDetails": {},
                    "edgeLabelDetails": {},
                    "labelTriples": [],
                }
            }
        ]
        failed = PGSchema.from_json(failed_payload)
        client = NeptuneClient("example.test")

        with patch(
            "neptune_schema_stats.client.pg_schema.fetch_pg_schema",
            return_value=failed,
        ):
            result = wait_for_schema(
                client,
                poll_interval=1.0,
                timeout=10.0,
                sleep=lambda _s: None,
                monotonic=lambda: 0.0,
            )
        # Failed is terminal, so we return it (caller decides how to handle).
        assert result.state() is SchemaState.FAILED
        assert result.status.error_message == "insufficient memory"
