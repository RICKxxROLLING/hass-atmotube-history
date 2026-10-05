"""End-to-end sync tests: simulated device -> statistics, with the real recorder."""

from __future__ import annotations

import time
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from bleak.backends.device import BLEDevice
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import statistics_during_period
from homeassistant.const import CONF_ADDRESS
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.atmo_history import statistics as statistics_module
from custom_components.atmo_history.aggregate import hour_start
from custom_components.atmo_history.const import (
    CONF_DRY_RUN,
    CONF_INFLUX_BUCKET,
    CONF_INFLUX_ENABLED,
    CONF_INFLUX_ORG,
    CONF_INFLUX_TOKEN,
    CONF_INFLUX_URL,
    DOMAIN,
)
from custom_components.atmo_history.influx import InfluxError
from custom_components.atmo_history.manager import AtmoHistoryManager
from custom_components.atmo_history.protocol import run_history_transfer

from .fakes import DISCONNECT, FakeAtmotube, simple_batch

ADDRESS = "AA:BB:CC:DD:EE:FF"
T0 = hour_start(int(time.time())) - 2 * 86400
STAT_TEMP = f"{DOMAIN}:aabbccddeeff_temperature"
STAT_PM25 = f"{DOMAIN}:aabbccddeeff_pm25"

INFLUX = {
    CONF_INFLUX_ENABLED: True,
    CONF_INFLUX_URL: "http://influx:8086",
    CONF_INFLUX_TOKEN: "token",
    CONF_INFLUX_ORG: "org",
    CONF_INFLUX_BUCKET: "bucket",
}


class DeviceHolder:
    """Lets each test swap the simulated device between syncs."""

    device: FakeAtmotube | None = None
    reachable: bool = True


@pytest.fixture
def ble() -> Generator[DeviceHolder]:
    holder = DeviceHolder()

    async def fake_download(ble_device: BLEDevice, name: str, on_batch: Any):
        assert holder.device is not None
        return await run_history_transfer(holder.device, on_batch, now=lambda: int(time.time()))

    with (
        patch(
            "custom_components.atmo_history.manager.async_download_history",
            side_effect=fake_download,
        ),
        patch(
            "custom_components.atmo_history.manager.bluetooth.async_ble_device_from_address",
            side_effect=lambda *a, **k: (
                BLEDevice(ADDRESS, "ATMOTUBE", None) if holder.reachable else None
            ),
        ),
    ):
        yield holder


async def setup_entry(
    hass: HomeAssistant, options: dict[str, Any] | None = None
) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=ADDRESS,
        title="Atmotube PRO EEFF",
        data={CONF_ADDRESS: ADDRESS},
        options=options or {},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


async def hour_stats(hass: HomeAssistant, statistic_id: str) -> list[dict[str, Any]]:
    await get_instance(hass).async_block_till_done()
    rows = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        datetime.fromtimestamp(T0 - 3600, UTC),
        None,
        {statistic_id},
        "hour",
        None,
        {"mean", "min", "max"},
    )
    return [
        {k: row[k] for k in ("start", "mean", "min", "max")} for row in rows.get(statistic_id, [])
    ]


def sensor_state(hass: HomeAssistant, key: str) -> str:
    entity_id = er.async_get(hass).async_get_entity_id("sensor", DOMAIN, f"{ADDRESS}_{key}")
    assert entity_id
    return hass.states.get(entity_id).state


async def sync(hass: HomeAssistant, entry: MockConfigEntry) -> AtmoHistoryManager:
    manager: AtmoHistoryManager = entry.runtime_data
    await manager.async_sync("test")
    await hass.async_block_till_done()
    return manager


async def test_interval_confirmed_from_consecutive_batches(
    hass: HomeAssistant, ble: DeviceHolder
) -> None:
    ble.device = FakeAtmotube(
        [simple_batch(T0, [10, 20, 30]), simple_batch(T0 + 180, [40, 50, 60])]
    )
    entry = await setup_entry(hass)
    manager = await sync(hass, entry)

    assert ble.device.acks == 2
    assert manager.status.last_result == "success"
    assert manager.status.last_records == 6
    assert manager.pending_batches == 0
    assert manager.interval_confirmed
    assert await hour_stats(hass, STAT_TEMP) == [
        {"start": T0, "mean": 35.0, "min": 10.0, "max": 60.0}
    ]
    assert (await hour_stats(hass, STAT_PM25))[0]["mean"] == 5.0
    assert sensor_state(hass, "last_result") == "success"
    assert sensor_state(hass, "records_imported") == "6"
    assert sensor_state(hass, "last_sync") != "unknown"


async def test_single_batch_held_until_confirmed(hass: HomeAssistant, ble: DeviceHolder) -> None:
    ble.device = FakeAtmotube([simple_batch(T0, [10, 20])])
    entry = await setup_entry(hass)
    manager = await sync(hass, entry)

    assert ble.device.acks == 1  # safely held in storage, so ACKed
    assert manager.status.last_result == "awaiting_interval_check"
    assert manager.pending_batches == 1
    assert await hour_stats(hass, STAT_TEMP) == []

    await manager.async_confirm_interval(60)
    await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done(wait_background_tasks=True)

    assert entry.runtime_data.pending_batches == 0
    assert await hour_stats(hass, STAT_TEMP) == [
        {"start": T0, "mean": 15.0, "min": 10.0, "max": 20.0}
    ]


async def test_interval_mismatch_stops_and_waits(hass: HomeAssistant, ble: DeviceHolder) -> None:
    ble.device = FakeAtmotube([simple_batch(T0, [10, 20]), simple_batch(T0 + 600, [30])])
    entry = await setup_entry(hass)
    manager = await sync(hass, entry)

    assert ble.device.acks == 1  # second batch not acknowledged
    assert manager.status.last_result == "interval_mismatch"
    assert "300.0 s" in manager.status.last_error
    assert manager.pending_batches == 1
    assert ir.async_get(hass).async_get_issue(DOMAIN, f"interval_mismatch_{entry.entry_id}")
    assert await hour_stats(hass, STAT_TEMP) == []

    # Blocked: advertisements no longer schedule syncs.
    manager._async_on_advertisement(None, None)
    assert manager._cancel_scheduled is None


async def test_dry_run_never_acks_or_stores(hass: HomeAssistant, ble: DeviceHolder) -> None:
    ble.device = FakeAtmotube([simple_batch(T0, [10, 20, 30])])
    entry = await setup_entry(hass, {CONF_DRY_RUN: True})
    manager = await sync(hass, entry)

    assert ble.device.ack_writes == []
    assert manager.status.last_result == "dry_run"
    assert manager.status.last_records == 3
    assert manager.pending_batches == 0
    assert await hour_stats(hass, STAT_TEMP) == []


async def test_no_data(hass: HomeAssistant, ble: DeviceHolder) -> None:
    ble.device = FakeAtmotube([])
    entry = await setup_entry(hass)
    manager = await sync(hass, entry)
    assert manager.status.last_result == "no_data"
    assert manager.status.last_records == 0


async def test_unreachable(hass: HomeAssistant, ble: DeviceHolder) -> None:
    ble.reachable = False
    entry = await setup_entry(hass)
    manager = await sync(hass, entry)
    assert manager.status.last_result == "device_unavailable"
    assert manager._failed


async def test_disconnect_mid_transfer(hass: HomeAssistant, ble: DeviceHolder) -> None:
    packets = simple_batch(T0, [10, 20, 30])
    ble.device = FakeAtmotube([[*packets[:2], DISCONNECT]])
    entry = await setup_entry(hass)
    manager = await sync(hass, entry)
    assert ble.device.ack_writes == []
    assert manager.status.last_result == "error"
    assert "disconnected" in manager.status.last_error
    assert manager.pending_batches == 0


async def test_influx_written_before_ack(hass: HomeAssistant, ble: DeviceHolder) -> None:
    ble.device = FakeAtmotube([simple_batch(T0, [10, 20])])
    entry = await setup_entry(hass, INFLUX)
    await entry.runtime_data.async_confirm_interval(60)
    with patch("custom_components.atmo_history.manager.influx.async_write", AsyncMock()) as write:
        manager = await sync(hass, entry)
    assert manager.status.last_result == "success"
    lines = write.await_args.args[5]
    assert lines[0].startswith("atmotube_history,device=AA:BB:CC:DD:EE:FF temperature=10.0,")
    assert lines[0].endswith(f" {T0}")
    assert ble.device.acks == 1


async def test_influx_failure_keeps_batch_and_retries(
    hass: HomeAssistant, ble: DeviceHolder
) -> None:
    ble.device = FakeAtmotube([simple_batch(T0, [10, 20])])
    entry = await setup_entry(hass, INFLUX)
    await entry.runtime_data.async_confirm_interval(60)

    with patch(
        "custom_components.atmo_history.manager.influx.async_write",
        AsyncMock(side_effect=InfluxError("HTTP 503")),
    ):
        manager = await sync(hass, entry)
    # Safely held in storage first, so acknowledged; the import is retried.
    assert ble.device.acks == 1
    assert manager.status.last_result == "error"
    assert "HTTP 503" in manager.status.last_error
    assert manager.pending_batches == 1
    assert manager._failed

    ble.device = FakeAtmotube([])
    with patch("custom_components.atmo_history.manager.influx.async_write", AsyncMock()):
        manager = await sync(hass, entry)
    assert manager.status.last_result == "success"
    assert manager.status.last_records == 2
    assert manager.pending_batches == 0
    # Statistics were written in both attempts but are not double counted.
    assert await hour_stats(hass, STAT_TEMP) == [
        {"start": T0, "mean": 15.0, "min": 10.0, "max": 20.0}
    ]


async def test_ack_does_not_wait_for_import(hass: HomeAssistant, ble: DeviceHolder) -> None:
    ble.device = FakeAtmotube([simple_batch(T0, [10, 20])])
    entry = await setup_entry(hass)
    await entry.runtime_data.async_confirm_interval(60)
    acks_at_import: list[int] = []

    real_import = statistics_module.async_import_records

    async def slow_import(*args: Any, **kwargs: Any) -> int:
        acks_at_import.append(ble.device.acks)
        return await real_import(*args, **kwargs)

    with patch(
        "custom_components.atmo_history.manager.async_import_records", side_effect=slow_import
    ):
        manager = await sync(hass, entry)
    assert acks_at_import == [1]
    assert manager.status.last_result == "success"


async def test_resend_in_same_session_imports_only_new_records(
    hass: HomeAssistant, ble: DeviceHolder
) -> None:
    first = int(time.time()) - 3 * 60
    ble.device = FakeAtmotube(
        [
            simple_batch(first, [10, 20, 30]),
            simple_batch(first, [10, 20, 30, 40]),  # ACK not applied: resent + 1 new
            simple_batch(first, [10, 20, 30, 40]),  # and again, nothing new
        ]
    )
    entry = await setup_entry(hass)
    manager = await sync(hass, entry)

    assert manager.status.last_result == "success"
    assert manager.status.last_records == 4
    assert ble.device.acks == 2  # stops at the second resend, which has nothing new
    total = sum(bucket.metrics["temperature"][0] for bucket in manager._aggregator.buckets.values())
    assert total == 4


async def test_resend_in_later_session_with_shifted_start(
    hass: HomeAssistant, ble: DeviceHolder
) -> None:
    entry = await setup_entry(hass)
    await entry.runtime_data.async_confirm_interval(60)
    ble.device = FakeAtmotube([simple_batch(T0, [10, 20, 30])])
    await sync(hass, entry)

    # Next visit: the device back-dates differently (57 s later) and resends.
    ble.device = FakeAtmotube([simple_batch(T0 + 57, [10, 20, 30, 40, 50])])
    manager = await sync(hass, entry)
    assert manager.status.last_result == "success"
    assert manager.status.last_records == 2
    assert await hour_stats(hass, STAT_TEMP) == [
        {"start": T0, "mean": 30.0, "min": 10.0, "max": 50.0}
    ]


async def test_resend_after_upgrade_without_stored_payload(
    hass: HomeAssistant, ble: DeviceHolder
) -> None:
    entry = await setup_entry(hass)
    manager: AtmoHistoryManager = entry.runtime_data
    await manager.async_confirm_interval(60)
    manager._last_batch = {"first_timestamp": T0, "record_count": 2}  # 0.1.1 format
    ble.device = FakeAtmotube([simple_batch(T0 + 57, [10, 20, 30])])
    manager = await sync(hass, entry)
    assert manager.status.last_result == "success"
    assert manager.status.last_records == 1


async def test_resend_with_different_data_is_rejected(
    hass: HomeAssistant, ble: DeviceHolder
) -> None:
    entry = await setup_entry(hass)
    await entry.runtime_data.async_confirm_interval(60)
    ble.device = FakeAtmotube([simple_batch(T0, [10, 20, 30])])
    await sync(hass, entry)
    ble.device = FakeAtmotube([simple_batch(T0 + 10, [99, 20, 30, 40])])
    manager = await sync(hass, entry)
    assert manager.status.last_result == "error"
    assert "different data" in manager.status.last_error
    assert ble.device.ack_writes == []


async def test_hour_merged_across_syncs(hass: HomeAssistant, ble: DeviceHolder) -> None:
    entry = await setup_entry(hass)
    await entry.runtime_data.async_confirm_interval(60)

    ble.device = FakeAtmotube([simple_batch(T0 + 1800, [10, 20])])
    await sync(hass, entry)
    ble.device = FakeAtmotube([simple_batch(T0 + 1920, [30, 40])])
    manager = await sync(hass, entry)

    assert manager.status.last_result == "success"
    assert await hour_stats(hass, STAT_TEMP) == [
        {"start": T0, "mean": 25.0, "min": 10.0, "max": 40.0}
    ]


async def test_gap_after_confirmation_continues(hass: HomeAssistant, ble: DeviceHolder) -> None:
    entry = await setup_entry(hass)
    await entry.runtime_data.async_confirm_interval(60)
    ble.device = FakeAtmotube([simple_batch(T0, [10]), simple_batch(T0 + 7200, [30])])
    manager = await sync(hass, entry)
    assert ble.device.acks == 2
    assert manager.status.last_result == "success"
    assert len(await hour_stats(hass, STAT_TEMP)) == 2


async def test_state_survives_restart(hass: HomeAssistant, ble: DeviceHolder) -> None:
    ble.device = FakeAtmotube([simple_batch(T0, [10])])
    entry = await setup_entry(hass)
    await entry.runtime_data.async_confirm_interval(60)
    await sync(hass, entry)
    await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    manager: AtmoHistoryManager = entry.runtime_data
    assert manager.status.last_result == "success"
    assert manager.interval_confirmed
    assert sensor_state(hass, "records_imported") == "1"


async def test_sync_now_service(hass: HomeAssistant, ble: DeviceHolder) -> None:
    ble.device = FakeAtmotube([])
    entry = await setup_entry(hass)
    await hass.services.async_call(DOMAIN, "sync_now", {}, blocking=True)
    assert entry.runtime_data.status.last_result == "no_data"

    ble.reachable = False
    with pytest.raises(HomeAssistantError, match="No connectable"):
        await hass.services.async_call(
            DOMAIN, "sync_now", {"config_entry_id": entry.entry_id}, blocking=True
        )


async def test_interval_confirmed_when_newest_batch_ends_now(
    hass: HomeAssistant, ble: DeviceHolder
) -> None:
    first = int(time.time()) - 3 * 60
    ble.device = FakeAtmotube([simple_batch(first, [10, 20, 30])])
    entry = await setup_entry(hass)
    manager = await sync(hass, entry)
    assert manager.status.last_result == "success"
    assert manager.interval_confirmed
    assert manager.pending_batches == 0
    assert manager.status.last_records == 3
