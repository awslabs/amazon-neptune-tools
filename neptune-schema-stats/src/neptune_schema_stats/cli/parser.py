"""Argument parser definition — every user-visible CLI flag lives here."""

from __future__ import annotations

import argparse

from neptune_schema_stats import __version__
from neptune_schema_stats.client.pg_schema import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_POLL_TIMEOUT_SECONDS,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="neptune-schema-stats",
        description=(
            "Per-label structural statistics for Neptune property-graph and RDF clusters."
        ),
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    # Connection
    conn = parser.add_argument_group("connection")
    conn.add_argument(
        "--endpoint",
        required=True,
        help="Neptune cluster endpoint hostname.",
    )
    conn.add_argument("--port", type=int, default=8182, help="Neptune port (default: 8182).")
    conn.add_argument(
        "--timeout", type=float, default=30.0, help="HTTP timeout in seconds (default: 30)."
    )
    conn.add_argument(
        "--no-verify-tls",
        action="store_true",
        help="Disable TLS certificate verification.",
    )

    # Auth
    auth = parser.add_argument_group("authentication")
    auth.add_argument(
        "--iam",
        action="store_true",
        help="Sign requests with AWS SigV4. Required for IAM-authenticated clusters.",
    )
    auth.add_argument("--region", help="AWS region for SigV4 signing (with --iam).")
    auth.add_argument(
        "--profile",
        help="AWS credentials profile for SigV4 signing (with --iam).",
    )

    # Mode
    mode = parser.add_argument_group("graph mode")
    mode.add_argument(
        "--mode",
        choices=["auto", "pg", "rdf"],
        default="auto",
        help="Graph model: auto (default), pg, or rdf.",
    )

    # Refresh
    refresh_group = parser.add_argument_group("refresh")
    refresh_group.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh cluster statistics (and PG schema in PG mode) before reporting.",
    )
    refresh_group.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help=f"Seconds between refresh polls (default: {DEFAULT_POLL_INTERVAL_SECONDS:g}).",
    )
    refresh_group.add_argument(
        "--poll-timeout",
        type=float,
        default=DEFAULT_POLL_TIMEOUT_SECONDS,
        help=f"Max seconds to wait for refresh (default: {DEFAULT_POLL_TIMEOUT_SECONDS:g}).",
    )

    # Multi-label detection (PG-only; on by default, cheap)
    multilabel = parser.add_argument_group("multi-label detection (PG-only)")
    multilabel.add_argument(
        "--skip-multi-label-check",
        action="store_true",
        help="Skip the pairwise multi-label probe.",
    )

    # Scan controls (PG-only)
    scans = parser.add_argument_group("scan controls (PG-only)")
    scans.add_argument(
        "--api-only",
        action="store_true",
        help="Skip scan queries; report ambiguous labels as ranges instead of exact counts.",
    )

    # Output
    out = parser.add_argument_group("output")
    out.add_argument(
        "--dump",
        action="store_true",
        help="Dump raw parsed API responses as JSON (skips correlation).",
    )
    out.add_argument(
        "--json",
        action="store_true",
        help="Emit the report as JSON.",
    )
    out.add_argument(
        "--details",
        action="store_true",
        help="Show the extended report (ambiguity ranges, warnings, structure detail).",
    )
    out.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress progress messages on stderr.",
    )
    out.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase logging verbosity (-v, -vv).",
    )

    return parser
