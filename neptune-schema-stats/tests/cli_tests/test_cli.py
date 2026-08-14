"""Tests for the CLI entry point."""

from __future__ import annotations

import json
from unittest.mock import patch
from urllib.parse import urlparse

import pytest

from neptune_schema_stats.cli import (
    EXIT_ERROR,
    EXIT_STATS_UNAVAILABLE,
    EXIT_TIMEOUT,
    main,
)
from neptune_schema_stats.client.base import (
    NeptuneHTTPError,
    NeptuneStatisticsNotAvailableError,
)
from neptune_schema_stats.client.statistics import StatisticsInfo
from neptune_schema_stats.models import NeptuneAPIError, PGSchema


class TestCLIArgumentParsing:
    def test_endpoint_is_required(self, capsys):
        with pytest.raises(SystemExit):
            main([])
        stderr = capsys.readouterr().err
        assert "--endpoint" in stderr

    def test_iam_requires_region(self, capsys):
        with pytest.raises(SystemExit):
            main(["--endpoint", "example.test", "--iam"])
        assert "--region" in capsys.readouterr().err


class TestCLIExecution:
    def test_forced_pg_mode_renders_table_by_default(self, capsys, pg_summary_json, pg_schema_json):
        argv = [
            "--endpoint",
            "example.test",
            "--mode",
            "pg",
            "--skip-multi-label-check",
            "--api-only",
            "--details",
        ]
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
        ):
            code = main(argv)
        captured = capsys.readouterr()
        assert code == 0
        # Table output — not JSON
        assert "Property Graph Statistics" in captured.out
        assert "airport" in captured.out
        assert "continent | country" in captured.out
        # Skipped, so no multi-label section
        assert "Multi-label detection" not in captured.out

    def test_pg_dump_flag_emits_raw_json(self, capsys, pg_summary_json, pg_schema_json):
        argv = ["--endpoint", "example.test", "--mode", "pg", "--dump"]
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
        ):
            code = main(argv)
        captured = capsys.readouterr()
        assert code == 0
        payload = json.loads(captured.out)
        assert payload["pg_summary"]["num_nodes"] == 3748
        assert "pg_schema" in payload

    def test_pg_json_flag_emits_correlated_json(self, capsys, pg_summary_json, pg_schema_json):
        argv = [
            "--endpoint",
            "example.test",
            "--mode",
            "pg",
            "--json",
            "--skip-multi-label-check",
            "--api-only",
        ]
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
        ):
            code = main(argv)
        payload = json.loads(capsys.readouterr().out)
        assert code == 0
        assert payload["schema_version"] == "1.0"
        assert payload["mode"] == "pg"
        assert payload["totals"] == {"nodes": 3748, "edges": 51300}
        # Airport has an exact structure not shared with any other label →
        # exact count, no max_count field.
        airport = next(e for e in payload["node_labels"] if e["label"] == "airport")
        # In --api-only mode, airport is ranged (3503 min, up to 3586 max).
        assert airport["count"] == 3503
        assert airport["max_count"] == 3586
        # Continent is fully ambiguous → 0 lower bound, 244 upper bound.
        continent = next(e for e in payload["node_labels"] if e["label"] == "continent")
        assert continent["count"] == 0
        assert continent["max_count"] == 244
        # Multi-label section should not be present (probe skipped, no partition data).
        assert "multi_label_combinations" not in payload

    def test_pg_schema_not_usable_prints_hint(
        self, capsys, pg_summary_json, pg_schema_not_started_json
    ):
        argv = ["--endpoint", "example.test", "--mode", "pg"]
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_not_started_json),
            ),
        ):
            code = main(argv)
        stderr = capsys.readouterr().err
        assert code == EXIT_ERROR
        assert "not usable" in stderr
        assert "--refresh" in stderr
        assert "1.4.8.0" in stderr

    def test_forced_rdf_mode_prints_payload(self, capsys, rdf_summary_json):
        argv = ["--endpoint", "example.test", "--mode", "rdf", "--dump"]
        with patch(
            "neptune_schema_stats.cli.rdf_flow.fetch_rdf_summary",
            return_value=_parse_rdf_summary(rdf_summary_json),
        ):
            code = main(argv)
        captured = capsys.readouterr()
        assert code == 0
        payload = json.loads(captured.out)
        assert payload["mode"] == "rdf"
        assert payload["rdf_summary"]["num_quads"] == 158571

    def test_stats_unavailable_exit_code(self, capsys):
        argv = ["--endpoint", "example.test", "--mode", "pg"]
        err = NeptuneStatisticsNotAvailableError(
            NeptuneAPIError(code="StatisticsNotAvailableException", detailed_message="")
        )
        with patch("neptune_schema_stats.cli.pg_flow.fetch_pg_summary", side_effect=err):
            code = main(argv)
        assert code == EXIT_STATS_UNAVAILABLE
        assert "Statistics not available" in capsys.readouterr().err

    def test_api_error_exit_code(self, capsys):
        argv = ["--endpoint", "example.test", "--mode", "pg"]
        err = NeptuneHTTPError(
            NeptuneAPIError(code="AccessDeniedException", detailed_message="Not authorized")
        )
        with patch("neptune_schema_stats.cli.pg_flow.fetch_pg_summary", side_effect=err):
            code = main(argv)
        assert code == EXIT_ERROR
        assert "AccessDeniedException" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _parse_pg_summary(body):
    from neptune_schema_stats.models import PGSummary

    return PGSummary.from_json(body)


def _parse_pg_schema(body):
    from neptune_schema_stats.models import PGSchema

    return PGSchema.from_json(body)


def _parse_rdf_summary(body):
    from neptune_schema_stats.models import RDFSummary

    return RDFSummary.from_json(body)


class TestRefreshCLI:
    def test_refresh_triggers_and_continues_to_report(
        self, capsys, pg_summary_json, pg_schema_json
    ):
        """--refresh triggers a fresh compute, waits, and then runs the
        default report."""
        argv = [
            "--endpoint",
            "example.test",
            "--mode",
            "pg",
            "--refresh",
            "--skip-multi-label-check",
            "--api-only",
        ]
        completed = PGSchema.from_json(pg_schema_json)
        initial_stats = StatisticsInfo(
            active=True,
            auto_compute=True,
            statistics_id=1000,
            date="2026-01-01",
            signature_count=None,
            instance_count=None,
            predicate_count=None,
        )
        refreshed_stats = StatisticsInfo(
            active=True,
            auto_compute=True,
            statistics_id=1001,
            date="2026-01-02",
            signature_count=None,
            instance_count=None,
            predicate_count=None,
        )
        with (
            patch(
                "neptune_schema_stats.cli.entry.fetch_statistics",
                return_value=initial_stats,
            ),
            patch(
                "neptune_schema_stats.cli.entry.trigger_statistics_refresh",
                return_value={},
            ),
            patch(
                "neptune_schema_stats.cli.entry.wait_for_statistics_refresh",
                return_value=refreshed_stats,
            ),
            patch(
                "neptune_schema_stats.cli.entry.trigger_pg_schema_compute", return_value={}
            ) as trig,
            patch("neptune_schema_stats.cli.entry.wait_for_schema", return_value=completed),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=completed,
            ),
        ):
            code = main(argv)
        captured = capsys.readouterr()
        assert code == 0
        assert trig.called
        # The main report — not just the schema JSON — should have rendered.
        assert "Property Graph Statistics" in captured.out
        assert "Node labels" in captured.out

    def test_refresh_failed_state_exits_schema_not_ready(self, capsys, pg_schema_not_started_json):
        argv = [
            "--endpoint",
            "example.test",
            "--mode",
            "pg",
            "--refresh",
        ]
        failed_payload = dict(pg_schema_not_started_json)
        failed_payload["results"] = [
            {
                "schema": {
                    "status": {
                        "state": "Failed",
                        "concurrency": "16",
                        "lastComputedTimestamp": "",
                        "progressPercentage": "0",
                        "errorMessage": "oom",
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
        with (
            patch(
                "neptune_schema_stats.cli.entry.fetch_statistics",
                side_effect=NeptuneHTTPError(
                    NeptuneAPIError(
                        code="EndpointNotFoundException",
                        detailed_message="/pg/statistics not found",
                    )
                ),
            ),
            patch("neptune_schema_stats.cli.entry.trigger_pg_schema_compute", return_value={}),
            patch("neptune_schema_stats.cli.entry.wait_for_schema", return_value=failed),
        ):
            code = main(argv)
        assert code == EXIT_ERROR
        err = capsys.readouterr().err
        assert "Failed" in err
        assert "oom" in err

    def test_refresh_timeout(self, capsys):
        argv = [
            "--endpoint",
            "example.test",
            "--mode",
            "pg",
            "--refresh",
            "--poll-timeout",
            "1",
        ]
        with (
            patch(
                "neptune_schema_stats.cli.entry.fetch_statistics",
                side_effect=NeptuneHTTPError(
                    NeptuneAPIError(
                        code="EndpointNotFoundException",
                        detailed_message="/pg/statistics not found",
                    )
                ),
            ),
            patch("neptune_schema_stats.cli.entry.trigger_pg_schema_compute", return_value={}),
            patch(
                "neptune_schema_stats.cli.entry.wait_for_schema",
                side_effect=TimeoutError("pg_schema did not reach a terminal state"),
            ),
        ):
            code = main(argv)
        assert code == EXIT_TIMEOUT
        assert "pg_schema" in capsys.readouterr().err

    def test_refresh_works_with_rdf_mode(self, capsys):
        """--refresh + --mode rdf is now supported (refreshes /rdf/statistics)."""
        from neptune_schema_stats.client.base import NeptuneHTTPError
        from neptune_schema_stats.client.statistics import StatisticsInfo

        initial = StatisticsInfo(
            active=True,
            auto_compute=True,
            statistics_id=1,
            date="2026-01-01",
            signature_count=None,
            instance_count=None,
            predicate_count=None,
        )
        refreshed = StatisticsInfo(
            active=True,
            auto_compute=True,
            statistics_id=2,
            date="2026-01-02",
            signature_count=None,
            instance_count=None,
            predicate_count=None,
        )
        with (
            patch(
                "neptune_schema_stats.cli.entry.fetch_statistics",
                return_value=initial,
            ),
            patch(
                "neptune_schema_stats.cli.entry.trigger_statistics_refresh",
                return_value={},
            ),
            patch(
                "neptune_schema_stats.cli.entry.wait_for_statistics_refresh",
                return_value=refreshed,
            ),
            # After refresh, the RDF summary fetch will fail (endpoint not mocked);
            # we just want to prove refresh succeeded (didn't reject upfront).
            patch(
                "neptune_schema_stats.cli.rdf_flow.fetch_rdf_summary",
                side_effect=NeptuneHTTPError(
                    NeptuneAPIError(code="InternalFailureException", detailed_message="mocked")
                ),
            ),
        ):
            code = main(["--endpoint", "example.test", "--mode", "rdf", "--refresh"])
        # Refresh itself succeeded; RDF summary fetch is the next thing that errored.
        # The important assertion is: no "PG-only" argparse rejection.
        stderr = capsys.readouterr().err
        assert "PG-only" not in stderr
        _ = code  # non-zero due to mocked summary failure — that's fine


class TestErrorHints:
    def test_dfe_limit_reached_hint(self, capsys, pg_summary_json):
        """When /pg/statistics reports the DFE hit its limit, the tool should
        exit early with a hint pointing to the docs + a support-case recommendation."""
        from neptune_schema_stats.client.statistics import StatisticsInfo

        limit_stats = StatisticsInfo(
            active=False,
            auto_compute=True,
            statistics_id=1786474944255,
            date="2026-08-11T19:02:24.255000+00:00",
            signature_count=None,
            instance_count=None,
            predicate_count=None,
            note="Limit reached: Statistics are not available",
        )
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow._fetch_statistics_best_effort",
                return_value=limit_stats,
            ),
        ):
            code = main(["--endpoint", "example.test", "--mode", "pg"])
        stderr = capsys.readouterr().err
        # Doesn't propagate the summary — exits early with the limit hint.
        assert "Limit reached" in stderr
        assert "DFE statistics limit" in stderr
        assert "support case" in stderr.lower()
        # Doc URL surfaces so the user has somewhere to go.
        urls_in_stderr = [token for token in stderr.split() if "://" in token]
        assert any(urlparse(token).hostname == "docs.aws.amazon.com" for token in urls_in_stderr)
        _ = code  # exit code is a stats-unavailable variant; specifics tested elsewhere

    def test_old_engine_version_hint(self, capsys):
        argv = ["--endpoint", "example.test", "--mode", "pg", "--refresh"]
        err = NeptuneHTTPError(
            NeptuneAPIError(
                code="MalformedQueryException",
                detailed_message="Unknown procedure name: neptune.graph.pg_schema.compute",
            )
        )
        with (
            patch(
                "neptune_schema_stats.cli.entry.fetch_statistics",
                side_effect=NeptuneHTTPError(
                    NeptuneAPIError(
                        code="EndpointNotFoundException",
                        detailed_message="/pg/statistics not found",
                    )
                ),
            ),
            patch("neptune_schema_stats.cli.entry.trigger_pg_schema_compute", side_effect=err),
        ):
            code = main(argv)
        stderr = capsys.readouterr().err
        assert code == EXIT_ERROR
        assert "1.4.8.0" in stderr


class TestMultiLabelDefaultBehavior:
    def test_multi_label_probe_runs_by_default_in_pg_mode(
        self, capsys, pg_summary_json, pg_schema_json
    ):
        """The default PG flow should invoke the multi-label probe and include
        its result in the report — no opt-in required."""
        from neptune_schema_stats.multi_label import MultiLabelProbeResult

        empty_probe = MultiLabelProbeResult(pairs_checked=6, hits=())
        argv = ["--endpoint", "example.test", "--mode", "pg", "--details", "--api-only"]
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.probe_multi_label_pairs",
                return_value=empty_probe,
            ) as probe_mock,
        ):
            code = main(argv)
        out = capsys.readouterr().out
        assert code == 0
        probe_mock.assert_called_once()
        assert "Multi-label detection" in out
        assert "No multi-labeled nodes found" in out

    def test_multi_label_hits_are_reported(self, capsys, pg_summary_json, pg_schema_json):
        from neptune_schema_stats.multi_label import MultiLabelPair, MultiLabelProbeResult

        probe = MultiLabelProbeResult(
            pairs_checked=6,
            hits=(MultiLabelPair(labels=("airport", "version"), node_count=42),),
        )
        argv = ["--endpoint", "example.test", "--mode", "pg", "--details", "--api-only"]
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.probe_multi_label_pairs",
                return_value=probe,
            ),
        ):
            code = main(argv)
        out = capsys.readouterr().out
        assert code == 0
        assert "airport | version" in out
        assert "42" in out
        # Multi-label section header should still appear
        assert "Multi-label detection" in out

    def test_probe_failure_prints_warning_but_completes(
        self, capsys, pg_summary_json, pg_schema_json
    ):
        from neptune_schema_stats.client.base import NeptuneClientError

        argv = ["--endpoint", "example.test", "--mode", "pg", "--api-only"]
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.probe_multi_label_pairs",
                side_effect=NeptuneClientError("access denied"),
            ),
        ):
            code = main(argv)
        captured = capsys.readouterr()
        assert code == 0  # Correlation still succeeded
        assert "Warning: multi-label probe failed" in captured.err
        # Report body still rendered (without the multi-label section).
        assert "Property Graph Statistics" in captured.out

    def test_skip_flag_bypasses_probe_entirely(self, capsys, pg_summary_json, pg_schema_json):
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
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
            patch("neptune_schema_stats.cli.pg_flow.probe_multi_label_pairs") as probe_mock,
        ):
            code = main(argv)
        assert code == 0
        probe_mock.assert_not_called()
        assert "Multi-label detection" not in capsys.readouterr().out


class TestRDFDefaultBehavior:
    """Tests for the RDF default flow: correlator + class-count probe + report."""

    def test_rdf_default_renders_report(self, capsys, rdf_summary_json):
        argv = ["--endpoint", "example.test", "--mode", "rdf", "--details"]
        with (
            patch(
                "neptune_schema_stats.cli.rdf_flow.fetch_rdf_summary",
                return_value=_parse_rdf_summary(rdf_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.rdf_flow.sparql_class_counts",
                return_value={
                    "http://kelvinlawrence.net/air-routes/class/Airport": 3502,
                    "http://kelvinlawrence.net/air-routes/class/Country": 237,
                    "http://kelvinlawrence.net/air-routes/class/Continent": 7,
                    "http://kelvinlawrence.net/air-routes/class/Version": 1,
                },
            ) as probe_mock,
        ):
            code = main(argv)
        out = capsys.readouterr().out
        assert code == 0
        probe_mock.assert_called_once()
        # Standard sections
        assert "RDF Graph Statistics" in out
        assert "Subject typing" in out
        assert "Class distribution" in out
        assert "Predicates" in out
        assert "Subject characteristic sets" in out
        # Class counts rendered
        assert "Airport" in out
        assert "3,502" in out

    def test_class_count_probe_failure_prints_warning(self, capsys, rdf_summary_json):
        from neptune_schema_stats.client.base import NeptuneClientError

        argv = ["--endpoint", "example.test", "--mode", "rdf"]
        with (
            patch(
                "neptune_schema_stats.cli.rdf_flow.fetch_rdf_summary",
                return_value=_parse_rdf_summary(rdf_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.rdf_flow.sparql_class_counts",
                side_effect=NeptuneClientError("access denied"),
            ),
        ):
            code = main(argv)
        captured = capsys.readouterr()
        assert code == 0
        assert "warning: class-count probe failed" in captured.err
        # Report still renders (without class distribution).
        assert "RDF Graph Statistics" in captured.out
        assert "Class distribution" not in captured.out

    def test_rdf_json_flag_emits_correlated_json(self, capsys, rdf_summary_json):
        argv = ["--endpoint", "example.test", "--mode", "rdf", "--json"]
        with patch(
            "neptune_schema_stats.cli.rdf_flow.fetch_rdf_summary",
            return_value=_parse_rdf_summary(rdf_summary_json),
        ):
            code = main(argv)
        assert code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["mode"] == "rdf"
        assert payload["schema_version"] == "1.0"
        # Concise totals mirror the text report.
        assert payload["totals"]["distinct_subjects"] == 54403
        assert payload["totals"]["quads"] == 158571
        # Subject typing split appears at the top level.
        assert payload["subject_typing"]["typed"] == 3747
        # Predicate list is populated.
        assert payload["predicates"]
        assert all("local_name" in p and "uri" in p for p in payload["predicates"])


class TestScanCLIIntegration:
    """CLI wiring for scans (default) and --api-only (opt-out)."""

    def test_scan_runs_and_renders(self, capsys, pg_summary_json, pg_schema_json):
        argv = [
            "--endpoint",
            "example.test",
            "--mode",
            "pg",
            "--skip-multi-label-check",
            "--details",
        ]
        # Air-routes plan: query {continent, country, version}, derive airport.
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
            patch("neptune_schema_stats.cli.pg_flow.execute_scan") as scan_mock,
        ):
            from neptune_schema_stats.scan import (
                NodeCountScan,
                ScanPlan,
                ScanResults,
            )

            fake_plan = ScanPlan(
                node_labels_to_query=("airport", "continent", "country", "version"),
                edge_labels_to_query=(),
            )
            fake_scan = ScanResults(
                plan=fake_plan,
                node_scans=(
                    NodeCountScan(label="airport", exact_count=3503),
                    NodeCountScan(label="continent", exact_count=7),
                    NodeCountScan(label="country", exact_count=237),
                    NodeCountScan(label="version", exact_count=1),
                ),
            )
            scan_mock.return_value = fake_scan
            code = main(argv)
        out = capsys.readouterr().out
        assert code == 0
        scan_mock.assert_called_once()
        # Scan summary section should render.
        assert "Scan summary" in out
        # And ambiguous groups should now be flagged as resolved.
        assert "resolved via scan" in out

    def test_no_scan_when_api_only(self, capsys, pg_summary_json, pg_schema_json):
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
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
            patch("neptune_schema_stats.cli.pg_flow.execute_scan") as scan_mock,
        ):
            code = main(argv)
        assert code == 0
        scan_mock.assert_not_called()
        out = capsys.readouterr().out
        assert "Scan summary" not in out

    def test_scan_runs_by_default(self, capsys, pg_summary_json, pg_schema_json):
        """Without --api-only, the scan should run when there are range-bounded labels."""
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
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
            patch("neptune_schema_stats.cli.pg_flow.execute_scan") as scan_mock,
        ):
            from neptune_schema_stats.scan import ScanPlan, ScanResults

            scan_mock.return_value = ScanResults(
                plan=ScanPlan(node_labels_to_query=(), edge_labels_to_query=())
            )
            code = main(argv)
        assert code == 0
        scan_mock.assert_called_once()

    def test_scan_result_included_in_json_output(self, capsys, pg_summary_json, pg_schema_json):
        argv = [
            "--endpoint",
            "example.test",
            "--mode",
            "pg",
            "--skip-multi-label-check",
            "--json",
        ]
        with (
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_summary",
                return_value=_parse_pg_summary(pg_summary_json),
            ),
            patch(
                "neptune_schema_stats.cli.pg_flow.fetch_pg_schema",
                return_value=_parse_pg_schema(pg_schema_json),
            ),
            patch("neptune_schema_stats.cli.pg_flow.execute_scan") as scan_mock,
        ):
            from neptune_schema_stats.scan import (
                NodeCountScan,
                ScanPlan,
                ScanResults,
            )

            scan_mock.return_value = ScanResults(
                plan=ScanPlan(
                    node_labels_to_query=("continent",),
                    edge_labels_to_query=(),
                ),
                node_scans=(NodeCountScan(label="continent", exact_count=7),),
            )
            code = main(argv)
        assert code == 0
        payload = json.loads(capsys.readouterr().out)
        # In the concise JSON output, scan results manifest as *exact* counts
        # on the affected labels rather than a separate "scan" section.
        continent = next(e for e in payload["node_labels"] if e["label"] == "continent")
        assert continent["count"] == 7
        assert "max_count" not in continent  # exact now, no range


class TestRDFFallback:
    """When DFE stats are unusable in RDF mode, the tool should fall back
    to SPARQL aggregate queries instead of erroring out."""

    def test_limit_reached_triggers_sparql_fallback(self, capsys):
        from neptune_schema_stats.client.statistics import StatisticsInfo
        from neptune_schema_stats.fallback.rdf import RDFFallbackResult

        limit_stats = StatisticsInfo(
            active=False,
            auto_compute=True,
            statistics_id=1786474944255,
            date="2026-08-11T19:02:24.255000+00:00",
            signature_count=None,
            instance_count=None,
            predicate_count=None,
            note="Limit reached: Statistics are not available",
        )
        fallback = RDFFallbackResult(
            total_triples=93_481_818,
            distinct_subjects=17_090_811,
            distinct_predicates=64,
            class_counts={
                "http://.../Artist": 9_123_191,
                "http://.../Movie": 501_769,
            },
        )
        with (
            patch(
                "neptune_schema_stats.cli.rdf_flow._fetch_statistics_best_effort",
                return_value=limit_stats,
            ),
            patch(
                "neptune_schema_stats.cli.rdf_flow.fetch_rdf_fallback",
                return_value=fallback,
            ),
        ):
            code = main(["--endpoint", "example.test", "--mode", "rdf"])
        out = capsys.readouterr().out
        stderr = capsys.readouterr().err
        assert code == 0
        # Fallback report rendered
        assert "RDF Graph Statistics (fallback mode)" in out
        assert "93,481,818" in out  # total triples
        assert "17,090,811" in out  # distinct subjects
        assert "Artist" in out  # class distribution
        _ = stderr  # limit hint was printed to stderr — not the point of this test

    def test_api_only_disables_fallback(self, capsys):
        """--api-only preserves 'no I/O beyond metadata APIs' — exit
        with the limit hint instead of running SPARQL queries."""
        from neptune_schema_stats.client.statistics import StatisticsInfo

        limit_stats = StatisticsInfo(
            active=False,
            auto_compute=True,
            statistics_id=1,
            date=None,
            signature_count=None,
            instance_count=None,
            predicate_count=None,
            note="Limit reached: Statistics are not available",
        )
        with (
            patch(
                "neptune_schema_stats.cli.rdf_flow._fetch_statistics_best_effort",
                return_value=limit_stats,
            ),
            patch(
                "neptune_schema_stats.cli.rdf_flow.fetch_rdf_fallback",
            ) as fallback_mock,
        ):
            code = main(["--endpoint", "example.test", "--mode", "rdf", "--api-only"])
        assert code != 0
        # No fallback query ran
        assert not fallback_mock.called
        # Limit hint printed to stderr
        assert "Limit reached" in capsys.readouterr().err

    def test_statistics_not_available_triggers_fallback(self, capsys):
        """When the RDF summary itself raises StatisticsNotAvailable, the
        error handler routes to the SPARQL fallback."""
        from neptune_schema_stats.client.base import NeptuneStatisticsNotAvailableError
        from neptune_schema_stats.fallback.rdf import RDFFallbackResult
        from neptune_schema_stats.models import NeptuneAPIError

        fallback = RDFFallbackResult(
            total_triples=100,
            distinct_subjects=50,
            distinct_predicates=10,
        )
        with (
            patch(
                "neptune_schema_stats.cli.rdf_flow._fetch_statistics_best_effort",
                return_value=None,
            ),
            patch(
                "neptune_schema_stats.cli.rdf_flow.fetch_rdf_summary",
                side_effect=NeptuneStatisticsNotAvailableError(
                    NeptuneAPIError(
                        code="StatisticsNotAvailableException",
                        detailed_message="statistics not computed yet",
                    )
                ),
            ),
            patch(
                "neptune_schema_stats.cli.rdf_flow.fetch_rdf_fallback",
                return_value=fallback,
            ),
        ):
            code = main(["--endpoint", "example.test", "--mode", "rdf"])
        assert code == 0
        out = capsys.readouterr().out
        assert "RDF Graph Statistics (fallback mode)" in out
        assert "100" in out  # total triples
