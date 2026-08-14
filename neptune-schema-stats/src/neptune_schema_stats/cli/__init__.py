"""CLI for neptune-schema-stats."""

from neptune_schema_stats.cli._constants import (
    EXIT_ERROR,
    EXIT_OK,
    EXIT_STATS_UNAVAILABLE,
    EXIT_TIMEOUT,
    EXIT_USAGE,
)
from neptune_schema_stats.cli.entry import main

__all__ = [
    "EXIT_ERROR",
    "EXIT_OK",
    "EXIT_STATS_UNAVAILABLE",
    "EXIT_TIMEOUT",
    "EXIT_USAGE",
    "main",
]
