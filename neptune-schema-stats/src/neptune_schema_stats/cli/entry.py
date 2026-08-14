"""Main entry point — parses args, drives the run, prints results.

Structure:

- JSON emit helpers (:func:`_to_jsonable`, :func:`_emit_json`)
- ``--refresh`` flow (:func:`_refresh_and_wait`)
- Orchestration (:func:`main`, :func:`_run`, :func:`_run_default`)
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import sys
from typing import Any

from neptune_schema_stats.cli._constants import (
    EXIT_ERROR,
    EXIT_OK,
    EXIT_STATS_UNAVAILABLE,
    EXIT_TIMEOUT,
    JSON_SCHEMA_VERSION,
)
from neptune_schema_stats.cli.hints import (
    _check_statistics_limit,
    _handle_http_error,
    _print_schema_not_usable_hint,
    _print_schema_status,
    _print_statistics_status,
)
from neptune_schema_stats.cli.parser import build_parser
from neptune_schema_stats.client import (
    IAMAuthConfig,
    NeptuneClient,
    NeptuneClientError,
    NeptuneHTTPError,
    NeptuneStatisticsNotAvailableError,
    trigger_pg_schema_compute,
    wait_for_schema,
)
from neptune_schema_stats.client.statistics import (
    PG_STATISTICS_PATH,
    RDF_STATISTICS_PATH,
    fetch_statistics,
    is_statistics_unavailable_error,
    trigger_statistics_refresh,
    wait_for_statistics_refresh,
)
from neptune_schema_stats.detect import ModeDetectionError, detect_mode
from neptune_schema_stats.models import GraphMode, SchemaState

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# JSON emit helpers                                                           #
# --------------------------------------------------------------------------- #


def _to_jsonable(value: Any) -> Any:
    """Recursively convert dataclass / tuple structures into JSON-serializable primitives."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: _to_jsonable(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, tuple):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    return value


def _emit_json(payload: dict[str, Any]) -> int:
    """Write ``payload`` to stdout as pretty-printed JSON, prefixed with
    ``schema_version``. Returns :data:`EXIT_OK` for the caller to use as
    its own return value."""
    envelope = {"schema_version": JSON_SCHEMA_VERSION, **payload}
    json.dump(envelope, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    return EXIT_OK


# --------------------------------------------------------------------------- #
# Refresh flow                                                                #
# --------------------------------------------------------------------------- #


def _refresh_and_wait(client: NeptuneClient, args: argparse.Namespace) -> int:
    """Refresh Neptune's DFE statistics and, in PG mode, recompute the PG
    schema. Blocks until each stage reaches a terminal state, then returns
    :data:`EXIT_OK` so the caller can continue to the report flow.

    Mode behavior:

    - ``pg``: refresh ``/pg/statistics`` + ``CALL neptune.graph.pg_schema.compute()``.
    - ``rdf``: refresh ``/rdf/statistics``. RDF has no separate schema API.
    - ``auto``: try PG first; if the PG statistics endpoint isn\'t available
      (or fails cleanly with "not found"), fall back to RDF statistics.
      Schema compute is skipped in this ambiguous case — users who want it
      should pass ``--mode pg`` explicitly.

    The statistics refresh comes first because the Graph Summary API
    (which feeds the report) is derived from the statistics engine; a
    stale — or, for a freshly loaded cluster, unavailable — ``statisticsId``
    means the summary itself may be unavailable.
    """
    try:
        if args.mode == "rdf":
            paths = [RDF_STATISTICS_PATH]
        elif args.mode == "pg":
            paths = [PG_STATISTICS_PATH]
        else:  # auto
            paths = [PG_STATISTICS_PATH, RDF_STATISTICS_PATH]

        refreshed_any = False
        pg_stats_succeeded = False
        for path in paths:
            try:
                initial_stats = fetch_statistics(client, path)
            except NeptuneHTTPError as exc:
                if is_statistics_unavailable_error(exc):
                    log.debug("%s not available on this cluster; skipping.", path)
                    continue
                raise

            log.info(
                "Statistics at %s: id=%s date=%s — triggering refresh",
                path,
                initial_stats.statistics_id,
                initial_stats.date,
            )
            trigger_statistics_refresh(client, path)
            log.info(
                "Polling %s every %.1fs (timeout %.0fs)",
                path,
                args.poll_interval,
                args.poll_timeout,
            )
            new_stats = wait_for_statistics_refresh(
                client,
                path,
                initial=initial_stats,
                poll_interval=args.poll_interval,
                timeout=args.poll_timeout,
                on_poll=lambda s: _print_statistics_status(s, prefix="poll"),
            )
            log.info(
                "Statistics refreshed at %s: id=%s date=%s",
                path,
                new_stats.statistics_id,
                new_stats.date,
            )
            # If the refreshed stats came back marked inactive (limit
            # reached, etc.), surface it now — the report would fail
            # downstream anyway, and the operator needs to know why.
            limit_exit = _check_statistics_limit(new_stats, path)
            if limit_exit is not None:
                return limit_exit
            refreshed_any = True
            if path == PG_STATISTICS_PATH:
                pg_stats_succeeded = True

        if not refreshed_any and args.mode != "pg":
            # In auto mode, if neither PG nor RDF statistics endpoint returned
            # anything, that\'s odd but not fatal — fall through and let
            # the normal report flow surface the mode-detection error.
            log.warning(
                "No statistics endpoints responded to --refresh. The cluster "
                "may be on an older engine that doesn't expose the endpoint."
            )

        # PG schema compute is PG-only. For RDF there is no equivalent
        # (RDF has no separate schema API — types are inline via rdf:type).
        # In auto mode, only run schema compute when we saw evidence this is
        # a PG cluster (PG statistics endpoint responded).
        if args.mode == "pg" or (args.mode == "auto" and pg_stats_succeeded):
            log.info("Triggering neptune.graph.pg_schema.compute()")
            trigger_pg_schema_compute(client)
            log.info(
                "Polling pg_schema every %.1fs (timeout %.0fs)",
                args.poll_interval,
                args.poll_timeout,
            )
            final = wait_for_schema(
                client,
                poll_interval=args.poll_interval,
                timeout=args.poll_timeout,
                on_poll=lambda s: _print_schema_status(s, prefix="poll"),
            )
            if final.state() is not SchemaState.COMPLETED:
                _print_schema_not_usable_hint(final, client.base_url)
                return EXIT_ERROR
            log.info("Schema Completed — continuing to report")
    except TimeoutError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_TIMEOUT
    except NeptuneHTTPError as exc:
        return _handle_http_error(exc)
    except NeptuneClientError as exc:
        print(f"Neptune client error: {exc}", file=sys.stderr)
        return EXIT_ERROR

    return EXIT_OK


# --------------------------------------------------------------------------- #
# Orchestration                                                               #
# --------------------------------------------------------------------------- #


def _configure_logging(verbosity: int, quiet: bool) -> None:
    """Configure logging output.

    Default behavior: INFO-level progress messages emitted to stderr with a
    simple ``> message`` prefix (no timestamps, no logger names). This gives
    users a live sense of what the tool is doing without cluttering output.

    ``--quiet`` suppresses INFO messages, leaving only warnings and errors.
    ``-v`` / ``-vv`` upgrade to DEBUG for troubleshooting (adds full
    timestamps and logger names).
    """
    if quiet:
        level = logging.WARNING
        fmt = "%(levelname)s: %(message)s"
    elif verbosity >= 2:
        level = logging.DEBUG
        fmt = "%(asctime)s %(levelname)-7s %(name)s - %(message)s"
    elif verbosity == 1:
        level = logging.DEBUG
        fmt = "%(levelname)s: %(message)s"
    else:
        level = logging.INFO
        # Prefix INFO messages with \'> \'; leave WARNING/ERROR clearly marked.
        fmt = "%(prefix)s%(message)s"

    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S")
    if not quiet and verbosity == 0:

        class _PrefixFilter(logging.Filter):
            def filter(self, record: logging.LogRecord) -> bool:
                record.prefix = "> " if record.levelno == logging.INFO else f"{record.levelname}: "
                return True

        logging.getLogger().handlers[0].addFilter(_PrefixFilter())


def _build_iam_config(args: argparse.Namespace) -> IAMAuthConfig | None:
    if not args.iam:
        return None
    return IAMAuthConfig(region=args.region, profile=args.profile)


def _resolve_mode(client: NeptuneClient, requested: str) -> GraphMode:
    if requested == "pg":
        return GraphMode.PG
    if requested == "rdf":
        return GraphMode.RDF
    return detect_mode(client)


def _run_default(client: NeptuneClient, args: argparse.Namespace) -> int:
    # Lazy imports break the entry↔flow circular dependency: flow modules
    # need helpers (`_emit_json`) defined here.
    from neptune_schema_stats.cli.pg_flow import _run_pg_default
    from neptune_schema_stats.cli.rdf_flow import _run_rdf_default, _run_rdf_fallback

    try:
        mode = _resolve_mode(client, args.mode)
    except ModeDetectionError as e:
        print(str(e), file=sys.stderr)
        return EXIT_ERROR

    log.info("Graph mode: %s", mode.value)

    try:
        if mode is GraphMode.PG:
            return _run_pg_default(client, args)
        return _run_rdf_default(client, args)
    except NeptuneStatisticsNotAvailableError as e:
        # If we\'re in RDF mode and the user didn\'t disable query I/O with
        # --api-only, fall back to SPARQL aggregates. Otherwise print the
        # actionable hint and exit.
        if mode is GraphMode.RDF and not args.api_only:
            log.warning(
                "RDF summary API is unavailable (%s). Falling back to SPARQL aggregate queries.",
                e,
            )
            return _run_rdf_fallback(client, args, reason=str(e))
        print(
            f"Statistics not available yet: {e}\n"
            "\n"
            "Neptune's Graph Summary API is derived from the DFE statistics engine.\n"
            "Re-run with --refresh to trigger a manual statistics recomputation and\n"
            "wait for it to complete before generating the report:\n"
            "\n"
            "    neptune-schema-stats --endpoint <host> --iam --region <r> --refresh\n",
            file=sys.stderr,
        )
        return EXIT_STATS_UNAVAILABLE
    except NeptuneHTTPError as e:
        return _handle_http_error(e)
    except NeptuneClientError as e:
        print(f"Neptune client error: {e}", file=sys.stderr)
        return EXIT_ERROR


def _run(args: argparse.Namespace) -> int:
    iam = _build_iam_config(args)
    client = NeptuneClient(
        endpoint=args.endpoint,
        port=args.port,
        iam=iam,
        timeout=args.timeout,
        verify_tls=not args.no_verify_tls,
    )
    log.info("Using endpoint %s", client.base_url)

    if args.refresh:
        exit_code = _refresh_and_wait(client, args)
        if exit_code != EXIT_OK:
            return exit_code
    return _run_default(client, args)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.iam and not args.region:
        parser.error("--iam requires --region")
    _configure_logging(args.verbose, args.quiet)
    try:
        return _run(args)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except BrokenPipeError:
        sys.stderr.close()
        return 0
    except Exception as exc:
        if args.verbose:
            raise
        print(
            f"Unexpected error: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        print(
            "Re-run with -v for a full traceback, or report this as a bug.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
