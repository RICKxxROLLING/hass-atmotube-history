"""Charging state from the device's advertisements."""

from __future__ import annotations

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.const import STATE_ON, STATE_UNAVAILABLE, STATE_UNKNOWN, EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import CONNECTION_BLUETOOTH, DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity

from .const import SIGNAL_STATUS
from .manager import AtmoHistoryConfigEntry, AtmoHistoryManager


async def async_setup_entry(
    hass: HomeAssistant,
    entry: AtmoHistoryConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up the charging sensor."""
    async_add_entities([AtmoChargingSensor(entry.runtime_data)])


class AtmoChargingSensor(BinarySensorEntity, RestoreEntity):
    """Whether the Atmotube is charging."""

    _attr_has_entity_name = True
    _attr_should_poll = False
    _attr_device_class = BinarySensorDeviceClass.BATTERY_CHARGING
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, manager: AtmoHistoryManager) -> None:
        """Initialize."""
        self._manager = manager
        self._attr_unique_id = f"{manager.address}_battery_charging"
        self._attr_device_info = DeviceInfo(
            connections={(CONNECTION_BLUETOOTH, manager.address)},
            name=manager.name,
            manufacturer="Atmotube",
            model="Atmotube PRO",
        )

    async def async_added_to_hass(self) -> None:
        """Restore the last state and follow advertisements."""
        if (last := await self.async_get_last_state()) is not None and last.state not in (
            STATE_UNKNOWN,
            STATE_UNAVAILABLE,
        ):
            self._attr_is_on = last.state == STATE_ON
        self._handle_status()
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                SIGNAL_STATUS.format(self._manager.entry.entry_id),
                self._handle_status,
            )
        )

    @callback
    def _handle_status(self) -> None:
        if (status := self._manager.device_status) is None:
            return
        self._attr_is_on = status.charging
        if self.hass is not None and self.entity_id:
            self.async_write_ha_state()
