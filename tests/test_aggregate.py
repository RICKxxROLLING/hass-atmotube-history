"""Tests for hourly aggregation."""

from __future__ import annotations

import pytest

from custom_components.atmo_history.aggregate import (
    INTERVAL_GAP,
    INTERVAL_MATCH,
    INTERVAL_MISMATCH,
    HourlyAggregator,
    HourStats,
    batch_ends_at,
    classify_interval,
    hour_start,
    measured_interval,
)
from custom_components.atmo_history.protocol import HistoryRecord

H = 1_700_000_000 - 1_700_000_000 % 3600


def rec(ts: int, temp: int, pm25: int | None = 5) -> HistoryRecord:
    return HistoryRecord(ts, temp, 50, 100, 101300, pm25, pm25, pm25)


def test_hour_start() -> None:
    assert hour_start(H + 3599) == H
    assert hour_start(H + 3600) == H + 3600


def test_mean_min_max_and_hours() -> None:
    agg = HourlyAggregator()
    touched, added = agg.add_records([rec(H, 10), rec(H + 60, 20), rec(H + 3600, 5)])
    assert touched == {H, H + 3600}
    assert added == 3
    assert agg.stats(H)["temperature"] == HourStats(15, 10, 20)
    assert agg.stats(H + 3600)["temperature"] == HourStats(5, 5, 5)


def test_resend_is_idempotent() -> None:
    agg = HourlyAggregator()
    agg.add_records([rec(H, 10), rec(H + 60, 20)])
    touched, added = agg.add_records([rec(H, 10), rec(H + 60, 20)])
    assert (touched, added) == (set(), 0)
    assert agg.stats(H)["temperature"] == HourStats(15, 10, 20)


def test_merge_partial_hour_across_batches() -> None:
    agg = HourlyAggregator()
    agg.add_records([rec(H, 10), rec(H + 60, 20)])
    agg.add_records([rec(H + 120, 30)])
    assert agg.stats(H)["temperature"] == HourStats(20, 10, 30)


def test_pm_off_skipped() -> None:
    agg = HourlyAggregator()
    agg.add_records([rec(H, 10, pm25=None), rec(H + 60, 10, pm25=8)])
    assert agg.stats(H)["pm25"] == HourStats(8, 8, 8)


def test_roundtrip_and_copy_isolation() -> None:
    agg = HourlyAggregator()
    agg.add_records([rec(H, 10)])
    clone = HourlyAggregator.from_dict(agg.as_dict())
    assert clone.stats(H) == agg.stats(H)
    copy = agg.copy()
    copy.add_records([rec(H + 60, 30)])
    assert agg.stats(H)["temperature"].mean == 10


def test_prune() -> None:
    agg = HourlyAggregator()
    agg.add_records([rec(H, 1), rec(H + 3600, 2)])
    agg.prune(H + 3600)
    assert list(agg.buckets) == [H + 3600]


def test_seed_from_database() -> None:
    agg = HourlyAggregator()
    agg.seed(H, {"temperature": HourStats(10, 5, 15)}, assumed_count=3)
    agg.add_records([rec(H + 60, 30)])
    assert agg.stats(H)["temperature"] == HourStats(15, 5, 30)


def test_measured_interval() -> None:
    assert measured_interval(H, 10, H + 600) == 60
    assert measured_interval(H, 0, H + 600) is None


@pytest.mark.parametrize(
    ("measured", "confirmed", "verdict"),
    [
        (60.0, False, INTERVAL_MATCH),
        (61.5, True, INTERVAL_MATCH),
        (300.0, False, INTERVAL_MISMATCH),
        (300.0, True, INTERVAL_GAP),
        (30.0, True, INTERVAL_MISMATCH),
        (-60.0, True, INTERVAL_MISMATCH),
    ],
)
def test_classify_interval(measured: float, confirmed: bool, verdict: str) -> None:
    assert classify_interval(measured, 60, confirmed) == verdict


def test_batch_ends_at() -> None:
    assert batch_ends_at(H, 37, 60, H + 37 * 60)
    assert batch_ends_at(H, 37, 60, H + 37 * 60 + 59)
    assert not batch_ends_at(H, 37, 120, H + 37 * 60)
    assert not batch_ends_at(H, 37, 30, H + 37 * 60)
    assert not batch_ends_at(H, 0, 60, H)
