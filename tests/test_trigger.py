"""Tests for the reappearance trigger and retry logic."""

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.components.bluetooth import BluetoothChange, BluetoothScanningMode
from homeassistant.const import CONF_ADDRESS
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components.atmo_history.const import DOMAIN
from custom_components.atmo_history.manager import AtmoHistoryManager

ADDRESS = "AA:BB:CC:DD:EE:FF"


class Clock:
    """Controllable monotonic clock."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.wall = dt_util.utcnow()

    def advance(self, seconds: float) -> None:
        self.now += seconds
        self.wall += timedelta(seconds=seconds)


@pytest.fixture
def clock() -> Generator[Clock]:
    clock = Clock()
    with patch(
        "custom_components.atmo_history.manager.monotonic",
        side_effect=lambda: clock.now,
    ):
        yield clock


@pytest.fixture
def registered() -> Generator[dict[str, Any]]:
    captured: dict[str, Any] = {}

    def fake_register(hass, callback, matcher, mode, **kwargs):
        captured.update(callback=callback, matcher=matcher, mode=mode, kwargs=kwargs)
        return MagicMock()

    with patch(
        "custom_components.atmo_history.manager.bluetooth.async_register_callback",
        side_effect=fake_register,
    ):
        yield captured


@pytest.fixture
def mock_sync() -> Generator[AsyncMock]:
    with patch.object(AtmoHistoryManager, "async_sync", autospec=True) as mock:
        yield mock


async def setup(hass: HomeAssistant, options: dict[str, Any] | None = None) -> AtmoHistoryManager:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=ADDRESS,
        data={CONF_ADDRESS: ADDRESS},
        title="Atmo",
        options=options or {},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry.runtime_data


async def advance(hass: HomeAssistant, clock: Clock, seconds: float) -> None:
    clock.advance(seconds)
    async_fire_time_changed(hass, clock.wall)
    await hass.async_block_till_done(wait_background_tasks=True)


def advertise(registered: dict[str, Any]) -> None:
    registered["callback"](MagicMock(), BluetoothChange.ADVERTISEMENT)


async def test_registers_for_device_address(
    hass: HomeAssistant, registered: dict[str, Any], clock: Clock
) -> None:
    await setup(hass)
    assert registered["matcher"]["address"] == ADDRESS
    assert registered["mode"] is BluetoothScanningMode.PASSIVE


async def test_first_sighting_syncs_after_delay(
    hass: HomeAssistant, registered: dict[str, Any], clock: Clock, mock_sync: AsyncMock
) -> None:
    await setup(hass)
    advertise(registered)
    await advance(hass, clock, 20)
    mock_sync.assert_not_called()
    await advance(hass, clock, 15)
    mock_sync.assert_called_once()
    assert mock_sync.call_args.args[1] == "reappeared"


async def test_reappearance_after_absence(
    hass: HomeAssistant, registered: dict[str, Any], clock: Clock, mock_sync: AsyncMock
) -> None:
    await setup(hass)
    advertise(registered)
    await advance(hass, clock, 31)
    assert mock_sync.call_count == 1

    # Still in range: frequent advertisements do not trigger.
    for _ in range(5):
        await advance(hass, clock, 60)
        advertise(registered)
    await advance(hass, clock, 31)
    assert mock_sync.call_count == 1

    # Gone for 9 minutes: not long enough.
    await advance(hass, clock, 9 * 60)
    advertise(registered)
    await advance(hass, clock, 31)
    assert mock_sync.call_count == 1

    # Gone for 10 minutes: sync again.
    await advance(hass, clock, 10 * 60)
    advertise(registered)
    await advance(hass, clock, 31)
    assert mock_sync.call_count == 2


async def test_retry_while_in_range_after_failure(
    hass: HomeAssistant, registered: dict[str, Any], clock: Clock, mock_sync: AsyncMock
) -> None:
    manager = await setup(hass, {"in_range_minutes": 0})
    advertise(registered)
    await advance(hass, clock, 31)
    assert mock_sync.call_count == 1
    manager._failed = True
    manager._last_attempt = clock.now

    for _ in range(2):  # 10 minutes of advertisements every 5 minutes
        await advance(hass, clock, 300)
        advertise(registered)
        await advance(hass, clock, 0)
    assert mock_sync.call_count == 1

    await advance(hass, clock, 300)  # 15 minutes since the failed attempt
    advertise(registered)
    await advance(hass, clock, 0)
    assert mock_sync.call_count == 2
    assert mock_sync.call_args.args[1] == "retry"


async def test_no_resync_after_success_when_periodic_off(
    hass: HomeAssistant, registered: dict[str, Any], clock: Clock, mock_sync: AsyncMock
) -> None:
    manager = await setup(hass, {"in_range_minutes": 0})
    advertise(registered)
    await advance(hass, clock, 31)
    manager._last_attempt = clock.now
    for _ in range(6):
        await advance(hass, clock, 300)
        advertise(registered)
        await advance(hass, clock, 0)
    assert mock_sync.call_count == 1


async def test_periodic_sync_while_in_range(
    hass: HomeAssistant, registered: dict[str, Any], clock: Clock, mock_sync: AsyncMock
) -> None:
    manager = await setup(hass)  # default: every 5 minutes
    advertise(registered)
    await advance(hass, clock, 31)
    assert mock_sync.call_count == 1
    manager._last_attempt = clock.now

    for _ in range(4):  # advertisements every minute, under 5 minutes
        await advance(hass, clock, 60)
        advertise(registered)
        await advance(hass, clock, 0)
    assert mock_sync.call_count == 1

    await advance(hass, clock, 60)
    advertise(registered)
    await advance(hass, clock, 0)
    assert mock_sync.call_count == 2
    assert mock_sync.call_args.args[1] == "periodic"


async def test_failed_sync_uses_retry_interval_not_periodic(
    hass: HomeAssistant, registered: dict[str, Any], clock: Clock, mock_sync: AsyncMock
) -> None:
    manager = await setup(hass)
    advertise(registered)
    await advance(hass, clock, 31)
    manager._failed = True
    manager._last_attempt = clock.now
    for _ in range(2):  # 10 minutes: past the 5-minute periodic, before the 15-minute retry
        await advance(hass, clock, 300)
        advertise(registered)
        await advance(hass, clock, 0)
    assert mock_sync.call_count == 1


async def test_unload_cancels_pending_sync(
    hass: HomeAssistant, registered: dict[str, Any], clock: Clock, mock_sync: AsyncMock
) -> None:
    manager = await setup(hass)
    advertise(registered)
    assert await hass.config_entries.async_unload(manager.entry.entry_id)
    await advance(hass, clock, 60)
    mock_sync.assert_not_called()
