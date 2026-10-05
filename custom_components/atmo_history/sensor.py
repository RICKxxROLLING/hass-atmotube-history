"""Sensors: minute-by-minute history values and sync diagnostics."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import (
    RestoreSensor,
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import PERCENTAGE, EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import CONNECTION_BLUETOOTH, DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import RESULTS, SIGNAL_STATUS, SIGNAL_UPDATED
from .manager import AtmoHistoryConfigEntry, AtmoHistoryManager
from .statistics import METRIC_META

HISTORY_DEVICE_CLASSES: dict[str, SensorDeviceClass] = {
    "temperature": SensorDeviceClass.TEMPERATURE,
    "humidity": SensorDeviceClass.HUMIDITY,
    "voc": SensorDeviceClass.VOLATILE_ORGANIC_COMPOUNDS_PARTS,
    "pressure": SensorDeviceClass.PRESSURE,
    "pm1": SensorDeviceClass.PM1,
    "pm25": SensorDeviceClass.PM25,
    "pm10": SensorDeviceClass.PM10,
}

# Decimals shown in the UI (in whatever unit the entity is displayed in).
HISTORY_DISPLAY_PRECISION: dict[str, int] = {
    "temperature": 1,
    "humidity": 0,
    "voc": 0,
    "pressure": 2,
    "pm1": 0,
    "pm25": 0,
    "pm10": 0,
}

HISTORY_SENSORS = tuple(
    SensorEntityDescription(
        key=f"{metric}_history",
        translation_key=f"{metric}_history",
        device_class=device_class,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=METRIC_META[metric][1],
        suggested_display_precision=HISTORY_DISPLAY_PRECISION[metric],
    )
    for metric, device_class in HISTORY_DEVICE_CLASSES.items()
)


@dataclass(frozen=True, kw_only=True)
class AtmoHistorySensorDescription(SensorEntityDescription):
    """Describes a diagnostic sensor."""

    value_fn: Callable[[AtmoHistoryManager], Any]
    attrs_fn: Callable[[AtmoHistoryManager], dict[str, Any]] | None = None


SENSORS = (
    AtmoHistorySensorDescription(
        key="last_sync",
        translation_key="last_sync",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=lambda m: m.status.last_success,
        attrs_fn=lambda m: {"last_attempt": m.status.last_attempt},
    ),
    AtmoHistorySensorDescription(
        key="records_imported",
        translation_key="records_imported",
        value_fn=lambda m: m.status.last_records,
        attrs_fn=lambda m: {"held_batches": m.pending_batches},
    ),
    AtmoHistorySensorDescription(
        key="last_result",
        translation_key="last_result",
        device_class=SensorDeviceClass.ENUM,
        options=RESULTS,
        value_fn=lambda m: m.status.last_result,
        attrs_fn=lambda m: {
            "error": m.status.last_error,
            "record_interval": m.interval,
            "interval_confirmed": m.interval_confirmed,
        },
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: AtmoHistoryConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up sensors."""
    manager = entry.runtime_data
    async_add_entities(
        [
            *(AtmoHistorySensor(manager, description) for description in SENSORS),
            *(AtmoHistoryValueSensor(manager, description) for description in HISTORY_SENSORS),
            AtmoBatterySensor(manager),
        ]
    )


def _device_info(manager: AtmoHistoryManager) -> DeviceInfo:
    return DeviceInfo(
        connections={(CONNECTION_BLUETOOTH, manager.address)},
        name=manager.name,
        manufacturer="Atmotube",
        model="Atmotube PRO",
    )


class AtmoHistoryValueSensor(RestoreSensor):
    """A measurement whose recorded history is the downloaded minute records.

    The state is the newest downloaded reading. Older readings are written
    into the recorder with their original timestamps by the manager.
    """

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, manager: AtmoHistoryManager, description: SensorEntityDescription) -> None:
        """Initialize."""
        self._manager = manager
        self.metric = description.key.removesuffix("_history")
        self.entity_description = description
        self._attr_unique_id = f"{manager.address}_{description.key}"
        self._attr_device_info = _device_info(manager)
        self._latest_ts = 0

    async def async_added_to_hass(self) -> None:
        """Restore the last value and register with the manager."""
        if (last := await self.async_get_last_sensor_data()) is not None:
            self._attr_native_value = last.native_value
        self._manager.history_entities[self.metric] = self
        self.async_on_remove(lambda: self._manager.history_entities.pop(self.metric, None))

    def render_state(self, value: float) -> str:
        """Return the state string HA would record for ``value``.

        Uses the entity's own conversion, so a display unit chosen in the
        entity settings (e.g. hPa instead of Pa) applies to backfilled rows.
        """
        saved = self._attr_native_value
        self._attr_native_value = value
        try:
            return str(self.state)
        finally:
            self._attr_native_value = saved

    def convert(self, value: float) -> float:
        """Convert a native value to the entity's display unit."""
        return float(self.render_state(value))

    def set_latest(self, value: float, timestamp: int) -> None:
        """Show the newest reading as the current state."""
        if timestamp < self._latest_ts:
            return
        self._latest_ts = timestamp
        self._attr_native_value = value
        self.async_write_ha_state()


class AtmoHistorySensor(SensorEntity):
    """A diagnostic sensor fed by the manager."""

    _attr_has_entity_name = True
    _attr_should_poll = False
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    entity_description: AtmoHistorySensorDescription

    def __init__(
        self, manager: AtmoHistoryManager, description: AtmoHistorySensorDescription
    ) -> None:
        """Initialize."""
        self._manager = manager
        self.entity_description = description
        self._attr_unique_id = f"{manager.address}_{description.key}"
        self._attr_device_info = _device_info(manager)

    async def async_added_to_hass(self) -> None:
        """Subscribe to updates."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                SIGNAL_UPDATED.format(self._manager.entry.entry_id),
                self._handle_update,
            )
        )

    @callback
    def _handle_update(self) -> None:
        self.async_write_ha_state()

    @property
    def native_value(self) -> Any:
        """Return the value."""
        return self.entity_description.value_fn(self._manager)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return extra attributes."""
        if self.entity_description.attrs_fn is None:
            return None
        return self.entity_description.attrs_fn(self._manager)


class AtmoBatterySensor(RestoreSensor):
    """Battery level from the device's advertisements."""

    _attr_has_entity_name = True
    _attr_should_poll = False
    _attr_device_class = SensorDeviceClass.BATTERY
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, manager: AtmoHistoryManager) -> None:
        """Initialize."""
        self._manager = manager
        self._attr_unique_id = f"{manager.address}_battery"
        self._attr_device_info = _device_info(manager)

    async def async_added_to_hass(self) -> None:
        """Restore the last level and follow advertisements."""
        if (last := await self.async_get_last_sensor_data()) is not None:
            self._attr_native_value = last.native_value
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
        self._attr_native_value = status.battery
        if self.hass is not None and self.entity_id:
            self.async_write_ha_state()
