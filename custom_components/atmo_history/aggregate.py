"""Hourly aggregation of history records with exact, idempotent merging.

Home Assistant's external statistics replace a whole hour on import, so we
keep the running count/sum/min/max for recent hours and which record times
they already contain. Re-importing a batch (e.g. after a lost ACK) is a no-op
and a batch that continues a partly imported hour extends it correctly.

No Home Assistant imports.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .protocol import HistoryRecord

METRICS: tuple[str, ...] = (
    "temperature",
    "humidity",
    "voc",
    "pressure",
    "pm1",
    "pm25",
    "pm10",
)

HOUR = 3600


def hour_start(timestamp: int) -> int:
    """Return the start of the UTC hour containing ``timestamp``."""
    return timestamp - timestamp % HOUR


@dataclass(frozen=True, slots=True)
class HourStats:
    """Mean/min/max of one metric over one hour."""

    mean: float
    min: float
    max: float


@dataclass
class HourBucket:
    """Accumulated values for one hour."""

    offsets: set[int] = field(default_factory=set)
    # metric -> [count, sum, min, max]
    metrics: dict[str, list[float]] = field(default_factory=dict)

    def add(self, offset: int, values: Mapping[str, float]) -> bool:
        """Add one record; return False if it was already counted."""
        if offset in self.offsets:
            return False
        self.offsets.add(offset)
        for key, value in values.items():
            acc = self.metrics.get(key)
            if acc is None:
                self.metrics[key] = [1, value, value, value]
            else:
                acc[0] += 1
                acc[1] += value
                acc[2] = min(acc[2], value)
                acc[3] = max(acc[3], value)
        return True

    def stats(self) -> dict[str, HourStats]:
        """Return per-metric statistics."""
        return {
            key: HourStats(mean=acc[1] / acc[0], min=acc[2], max=acc[3])
            for key, acc in self.metrics.items()
            if acc[0]
        }

    def as_dict(self) -> dict[str, Any]:
        """Serialize for storage."""
        return {"o": sorted(self.offsets), "m": self.metrics}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HourBucket:
        """Deserialize from storage."""
        return cls(
            offsets=set(data.get("o", ())),
            metrics={k: list(v) for k, v in data.get("m", {}).items()},
        )


class HourlyAggregator:
    """Collection of hour buckets keyed by hour start (Unix time)."""

    def __init__(self, buckets: dict[int, HourBucket] | None = None) -> None:
        """Initialize."""
        self.buckets: dict[int, HourBucket] = buckets or {}

    def add_records(self, records: Iterable[HistoryRecord]) -> tuple[set[int], int]:
        """Add records; return (touched hours, number of new records)."""
        touched: set[int] = set()
        added = 0
        for record in records:
            start = hour_start(record.timestamp)
            bucket = self.buckets.setdefault(start, HourBucket())
            if bucket.add(record.timestamp - start, record.values()):
                touched.add(start)
                added += 1
        return touched, added

    def seed(self, start: int, existing: Mapping[str, HourStats], assumed_count: int) -> None:
        """Seed an untracked hour from statistics already in the database.

        Only used for hours older than the tracking window, where the exact
        count is unknown. ``assumed_count`` weights the existing mean.
        """
        if start in self.buckets:
            return
        count = max(1, assumed_count)
        self.buckets[start] = HourBucket(
            metrics={
                key: [count, stats.mean * count, stats.min, stats.max]
                for key, stats in existing.items()
            }
        )

    def stats(self, start: int) -> dict[str, HourStats]:
        """Return statistics for one hour."""
        bucket = self.buckets.get(start)
        return bucket.stats() if bucket else {}

    def prune(self, before: int) -> None:
        """Forget hours starting before ``before``."""
        for start in [s for s in self.buckets if s < before]:
            del self.buckets[start]

    def as_dict(self) -> dict[str, Any]:
        """Serialize for storage."""
        return {str(k): v.as_dict() for k, v in self.buckets.items()}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> HourlyAggregator:
        """Deserialize from storage."""
        return cls({int(k): HourBucket.from_dict(v) for k, v in (data or {}).items()})

    def copy(self) -> HourlyAggregator:
        """Return a deep copy, so a failed import leaves the original intact."""
        return HourlyAggregator.from_dict(self.as_dict())


def measured_interval(previous_first: int, previous_count: int, next_first: int) -> float | None:
    """Infer the record interval from two consecutive HT headers."""
    if previous_count <= 0:
        return None
    return (next_first - previous_first) / previous_count


INTERVAL_TOLERANCE = 2.0  # seconds

INTERVAL_MATCH = "match"
INTERVAL_GAP = "gap"
INTERVAL_MISMATCH = "mismatch"


def classify_interval(measured: float, configured: int, confirmed: bool) -> str:
    """Compare a measured interval to the configured one.

    Before the interval is confirmed, anything but a match is a mismatch.
    Afterwards a longer spacing is a recording gap (e.g. the device was off),
    while a shorter one means records would overlap and is a mismatch.
    """
    if abs(measured - configured) <= INTERVAL_TOLERANCE:
        return INTERVAL_MATCH
    if confirmed and measured > configured:
        return INTERVAL_GAP
    return INTERVAL_MISMATCH


def batch_ends_at(first: int, count: int, interval: int, now: int) -> bool:
    """Return True if a batch's spacing puts its end at ``now``.

    The device records continuously, so when the newest batch is downloaded
    its next record is due within one interval of the sync time. This only
    ever confirms an interval; a miss can just mean older data.
    """
    return count > 0 and abs(first + count * interval - now) <= interval


def is_resend(prev_first: int, prev_count: int, new_first: int, interval: int) -> bool:
    """Return True if a batch header looks like the previous batch again.

    The device back-dates the first record from the time we send, so a
    re-sent batch starts within about one interval of the previous start,
    while a genuinely new batch starts where the previous one ended.
    """
    if prev_count <= 0:
        return False
    return abs(new_first - prev_first) < abs(new_first - (prev_first + prev_count * interval))
