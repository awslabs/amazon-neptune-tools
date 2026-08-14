"""Shared pytest fixtures."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict[str, Any]:
    with (FIXTURES_DIR / name).open("r", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture
def pg_summary_json() -> dict[str, Any]:
    return _load("air_routes_pg_summary.json")


@pytest.fixture
def pg_schema_json() -> dict[str, Any]:
    return _load("air_routes_pg_schema.json")


@pytest.fixture
def air_routes_summary(pg_summary_json):
    from neptune_schema_stats.models import PGSummary

    return PGSummary.from_json(pg_summary_json)


@pytest.fixture
def air_routes_schema(pg_schema_json):
    from neptune_schema_stats.models import PGSchema

    return PGSchema.from_json(pg_schema_json)


@pytest.fixture
def pg_schema_not_started_json() -> dict[str, Any]:
    return _load("pg_schema_not_started.json")


@pytest.fixture
def pg_schema_in_progress_json() -> dict[str, Any]:
    return _load("pg_schema_in_progress.json")


@pytest.fixture
def rdf_summary_json() -> dict[str, Any]:
    return _load("air_routes_rdf_summary.json")


@pytest.fixture
def stats_not_available_json() -> dict[str, Any]:
    return _load("error_stats_not_available.json")
