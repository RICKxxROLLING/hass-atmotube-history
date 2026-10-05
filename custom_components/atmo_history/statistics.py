"""Import history into Home Assistant long-term statistics."""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    statistics_during_period,
)
from homeassistant.const import (
    PERCENTAGE,
    UnitOfDensity,
    UnitOfPressure,
    UnitOfRatio,
    UnitOfTemperature,
)
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

from .aggregate import HOUR, METRICS, HourlyAggregator, HourStats, hour_start
from .const import DOMAIN, TRACKED_HOURS_DAYS
from .protocol import HistoryRecord

_LOGGER = logging.getLogger(__name__)

# metric -> (label, unit, unit_class). Units match the live sensors of the
# ha-atmo integration. unit_class follows what the sensor integration uses for
# the matching device class (None where HA has no converter).
METRIC_META: dict[str, tuple[str, str, str | None]] = {
    "temperature": ("Temperature", UnitOfTemperature.CELSIUS, "temperature"),
    "humidity": ("Humidity", PERCENTAGE, None),
    "voc": ("VOC", UnitOfRatio.PARTS_PER_BILLION, "unitless"),
    "pressure": ("Pressure", UnitOfPressure.PA, "pressure"),
    "pm1": ("PM1", UnitOfDensity.MICROGRAMS_PER_CUBIC_METER, None),
    "pm25": ("PM2.5", UnitOfDensity.MICROGRAMS_PER_CUBIC_METER, None),
    "pm10": ("PM10", UnitOfDensity.MICROGRAMS_PER_CUBIC_METER, None),
}
assert set(METRIC_META) == set(METRICS)


def statistic_id(address: str, metric: str) -> str:
    """Return the external statistic id for a metric."""
    return f"{DOMAIN}:{address.replace(':', '').lower()}_{metric}"


def metadata(address: str, name: str, metric: str) -> StatisticMetaData:
    """Return statistics metadata for a metric."""
    label, unit, unit_class = METRIC_META[metric]
    return StatisticMetaData(
        mean_type=StatisticMeanType.ARITHMETIC,
        has_sum=False,
        name=f"{name} {label} (history)",
        source=DOMAIN,
        statistic_id=statistic_id(address, metric),
        unit_class=unit_class,
        unit_of_measurement=unit,
    )


async def _async_seed_old_hours(
    hass: HomeAssistant,
    address: str,
    aggregator: HourlyAggregator,
    records: Sequence[HistoryRecord],
    interval: int,
    cutoff: int,
) -> None:
    """Merge with database rows for hours outside the tracking window."""
    new_per_hour = Counter(hour_start(r.timestamp) for r in records)
    old = sorted(h for h in new_per_hour if h < cutoff and h not in aggregator.buckets)
    if not old:
        return
    ids = {statistic_id(address, m): m for m in METRICS}
    rows = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        datetime.fromtimestamp(old[0], UTC),
        datetime.fromtimestamp(old[-1] + HOUR, UTC),
        set(ids),
        "hour",
        None,
        {"mean", "min", "max"},
    )
    existing: dict[int, dict[str, HourStats]] = {}
    for stat_id, stat_rows in rows.items():
        for row in stat_rows:
            if None in (row.get("mean"), row.get("min"), row.get("max")):
                continue
            existing.setdefault(int(row["start"]), {})[ids[stat_id]] = HourStats(
                mean=row["mean"], min=row["min"], max=row["max"]
            )
    expected = max(1, HOUR // interval)
    for start in old:
        if start in existing:
            _LOGGER.debug("Merging with untracked database hour %s", start)
            aggregator.seed(start, existing[start], expected - new_per_hour[start])


async def async_import_records(
    hass: HomeAssistant,
    address: str,
    name: str,
    aggregator: HourlyAggregator,
    records: Sequence[HistoryRecord],
    interval: int,
) -> tuple[int, set[int]]:
    """Merge records into hourly statistics and wait for the recorder commit.

    ``aggregator`` is updated in place; the caller persists it only after
    everything else has succeeded. Returns the number of new records and the
    hours (start timestamps) that changed.
    """
    cutoff = hour_start(int(dt_util.utcnow().timestamp())) - TRACKED_HOURS_DAYS * 86400
    await _async_seed_old_hours(hass, address, aggregator, records, interval, cutoff)

    touched, added = aggregator.add_records(records)
    if touched:
        per_metric: dict[str, list[StatisticData]] = {m: [] for m in METRICS}
        for start in sorted(touched):
            start_dt = datetime.fromtimestamp(start, UTC)
            for metric, stats in aggregator.stats(start).items():
                per_metric[metric].append(
                    StatisticData(start=start_dt, mean=stats.mean, min=stats.min, max=stats.max)
                )
        for metric, rows in per_metric.items():
            if rows:
                async_add_external_statistics(hass, metadata(address, name, metric), rows)
        await get_instance(hass).async_block_till_done()

    aggregator.prune(cutoff)
    return added, touched
