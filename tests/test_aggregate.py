"""Tests for hourly aggregation."""

from __future__ import annotations

import pytest

from custom_components.atmo_history.aggregate import (
    INTERVAL_GAP,
    INTERVAL_MATCH,
    INTERVAL_MISMATCH,
    INTERVAL_UNDECIDED,
    HourlyAggregator,
    HourStats,
    batch_ends_at,
    check_continuity,
    hour_start,
    is_resend,
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


@pytest.mark.parametrize(
    ("new_first", "prev_count", "confirmed", "verdict", "start"),
    [
        # Continues the previous batch exactly.
        (H + 600, 10, False, INTERVAL_MATCH, H + 600),
        # Back-dating jitter of up to one interval is re-anchored onto the grid.
        (H + 300 - 53, 5, True, INTERVAL_MATCH, H + 300),
        (H + 300 + 40, 5, True, INTERVAL_MATCH, H + 300),
        # Too few records to confirm an unconfirmed interval.
        (H + 300 - 53, 5, False, INTERVAL_UNDECIDED, H + 300),
        # A real gap once the interval is known.
        (H + 600 + 3600, 10, True, INTERVAL_GAP, H + 600 + 3600),
        (H + 600 + 3600, 10, False, INTERVAL_MISMATCH, H + 600 + 3600),
        # Starts well before the previous batch ended: overlap.
        (H + 300, 10, True, INTERVAL_MISMATCH, H + 300),
    ],
)
def test_check_continuity(
    new_first: int, prev_count: int, confirmed: bool, verdict: str, start: int
) -> None:
    assert check_continuity(H, prev_count, new_first, 60, confirmed) == (verdict, start)


def test_batch_ends_at() -> None:
    assert batch_ends_at(H, 37, 60, H + 37 * 60)
    assert batch_ends_at(H, 37, 60, H + 37 * 60 + 59)
    assert not batch_ends_at(H, 37, 120, H + 37 * 60)
    assert not batch_ends_at(H, 37, 30, H + 37 * 60)
    assert not batch_ends_at(H, 0, 60, H)


def test_is_resend() -> None:
    assert is_resend(H, 44, H, 60)
    assert is_resend(H, 44, H + 57, 60)
    assert not is_resend(H, 44, H + 44 * 60, 60)
    assert not is_resend(H, 44, H + 44 * 60 + 3600, 60)
    assert not is_resend(H, 0, H, 60)
