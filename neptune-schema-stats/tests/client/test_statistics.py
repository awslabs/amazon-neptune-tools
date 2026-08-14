from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from neptune_schema_stats.client.base import NeptuneHTTPError
from neptune_schema_stats.client.statistics import (
    PG_STATISTICS_PATH,
    RDF_STATISTICS_PATH,
    StatisticsInfo,
    fetch_statistics,
    is_statistics_unavailable_error,
    trigger_statistics_refresh,
    wait_for_statistics_refresh,
)
from neptune_schema_stats.models import NeptuneAPIError


class TestFetchStatistics:
    def test_parses_full_payload(self):
        mock = MagicMock()
        mock.get_pg_statistics.return_value = {
            "status": "200 OK",
            "payload": {
                "autoCompute": True,
                "active": True,
                "statisticsId": 1775344245114,
                "date": "2026-04-04T23:10:45.114000+00:00",
                "signatureInfo": {
                    "signatureCount": 398,
                    "instanceCount": 1718561,
                    "predicateCount": 40,
                },
            },
        }
        info = fetch_statistics(mock, PG_STATISTICS_PATH)
        assert info == StatisticsInfo(
            active=True,
            auto_compute=True,
            statistics_id=1775344245114,
            date="2026-04-04T23:10:45.114000+00:00",
            signature_count=398,
            instance_count=1718561,
            predicate_count=40,
        )
        mock.get_pg_statistics.assert_called_once_with()

    def test_missing_optional_fields_default_to_none(self):
        mock = MagicMock()
        mock.get_pg_statistics.return_value = {"payload": {"active": False, "autoCompute": False}}
        info = fetch_statistics(mock, PG_STATISTICS_PATH)
        assert info.active is False
        assert info.statistics_id is None
        assert info.date is None
        assert info.signature_count is None

    def test_rdf_path_routing(self):
        mock = MagicMock()
        mock.get_rdf_statistics.return_value = {"payload": {"active": False, "autoCompute": False}}
        fetch_statistics(mock, RDF_STATISTICS_PATH)
        mock.get_rdf_statistics.assert_called_once_with()


class TestTriggerStatisticsRefresh:
    def test_posts_refresh_mode(self):
        mock = MagicMock()
        mock.refresh_pg_statistics.return_value = {"payload": {"status": "acceptedQuery"}}
        result = trigger_statistics_refresh(mock, PG_STATISTICS_PATH)
        mock.refresh_pg_statistics.assert_called_once_with()
        assert result == {"payload": {"status": "acceptedQuery"}}

    def test_rdf_path_routing(self):
        mock = MagicMock()
        mock.refresh_rdf_statistics.return_value = {"payload": {}}
        trigger_statistics_refresh(mock, RDF_STATISTICS_PATH)
        mock.refresh_rdf_statistics.assert_called_once_with()


class TestWaitForStatisticsRefresh:
    def test_returns_when_id_changes(self):
        initial = StatisticsInfo(
            active=True,
            auto_compute=True,
            statistics_id=100,
            date=None,
            signature_count=None,
            instance_count=None,
            predicate_count=None,
        )
        mock = MagicMock()
        mock.get_pg_statistics.return_value = {
            "payload": {
                "active": True,
                "autoCompute": True,
                "statisticsId": 200,
                "date": None,
            }
        }
        result = wait_for_statistics_refresh(
            mock, PG_STATISTICS_PATH, initial=initial, poll_interval=0.01, timeout=5
        )
        assert result.statistics_id == 200

    def test_raises_timeout_if_id_never_changes(self):
        initial = StatisticsInfo(
            active=True,
            auto_compute=True,
            statistics_id=100,
            date=None,
            signature_count=None,
            instance_count=None,
            predicate_count=None,
        )
        mock = MagicMock()
        mock.get_pg_statistics.return_value = {
            "payload": {
                "active": True,
                "autoCompute": True,
                "statisticsId": 100,
                "date": None,
            }
        }
        with pytest.raises(TimeoutError, match="did not refresh"):
            wait_for_statistics_refresh(
                mock,
                PG_STATISTICS_PATH,
                initial=initial,
                poll_interval=0.01,
                timeout=0.05,
            )


class TestIsStatisticsUnavailableError:
    def test_endpoint_not_found_matches(self):
        exc = NeptuneHTTPError(
            NeptuneAPIError(
                code="EndpointNotFoundException",
                detailed_message="/pg/statistics not found",
            )
        )
        assert is_statistics_unavailable_error(exc) is True

    def test_generic_error_does_not_match(self):
        exc = NeptuneHTTPError(
            NeptuneAPIError(code="InternalFailureException", detailed_message="something else")
        )
        assert is_statistics_unavailable_error(exc) is False

    def test_non_http_error_does_not_match(self):
        assert is_statistics_unavailable_error(ValueError("boom")) is False


class TestLimitReached:
    """Neptune reports ``active: false`` + a ``note`` field when the DFE
    hits its characteristic-set / signature limit and can no longer
    compute statistics. This is a persistent state — the tool should
    detect it and print actionable remediation guidance."""

    def test_parses_note_and_flags_limit(self):
        mock = MagicMock()
        mock.get_pg_statistics.return_value = {
            "status": "200 OK",
            "payload": {
                "autoCompute": True,
                "active": False,
                "statisticsId": 1786474944255,
                "date": "2026-08-11T19:02:24.255000+00:00",
                "note": "Limit reached: Statistics are not available",
            },
        }
        info = fetch_statistics(mock, PG_STATISTICS_PATH)
        assert info.active is False
        assert info.note == "Limit reached: Statistics are not available"
        assert info.has_limit_note is True
        assert info.is_usable is False

    def test_inactive_without_limit_note_is_still_unusable(self):
        info = StatisticsInfo(
            active=False,
            auto_compute=True,
            statistics_id=1,
            date=None,
            signature_count=None,
            instance_count=None,
            predicate_count=None,
            note="Recomputing…",
        )
        assert info.has_limit_note is False
        assert info.is_usable is False

    def test_active_no_note_is_usable(self):
        info = StatisticsInfo(
            active=True,
            auto_compute=True,
            statistics_id=1,
            date="2026-01-01",
            signature_count=100,
            instance_count=1000,
            predicate_count=10,
        )
        assert info.has_limit_note is False
        assert info.is_usable is True

    def test_various_limit_note_wording_matched(self):
        for note in [
            "Limit reached: Statistics are not available",
            "The DFE statistics limit has been exceeded",
            "limit reached",
            "LIMIT REACHED - contact support",
        ]:
            info = StatisticsInfo(
                active=False,
                auto_compute=True,
                statistics_id=1,
                date=None,
                signature_count=None,
                instance_count=None,
                predicate_count=None,
                note=note,
            )
            assert info.has_limit_note is True, f"expected match on: {note}"
