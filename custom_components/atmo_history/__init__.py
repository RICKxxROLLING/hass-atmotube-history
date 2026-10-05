"""Atmotube PRO history backfill."""

from __future__ import annotations

import voluptuous as vol
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.typing import ConfigType

from .const import (
    ATTR_CONFIG_ENTRY_ID,
    DOMAIN,
    RESULT_AWAITING_INTERVAL,
    RESULT_DRY_RUN,
    RESULT_NO_DATA,
    RESULT_SUCCESS,
    SERVICE_SYNC_NOW,
)
from .manager import AtmoHistoryConfigEntry, AtmoHistoryManager

PLATFORMS = [Platform.BINARY_SENSOR, Platform.SENSOR]

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

SYNC_NOW_SCHEMA = vol.Schema({vol.Optional(ATTR_CONFIG_ENTRY_ID): cv.string})

OK_RESULTS = {RESULT_SUCCESS, RESULT_NO_DATA, RESULT_DRY_RUN, RESULT_AWAITING_INTERVAL}


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Register the sync_now service."""

    async def _async_sync_now(call: ServiceCall) -> None:
        entry_id = call.data.get(ATTR_CONFIG_ENTRY_ID)
        entries: list[AtmoHistoryConfigEntry] = [
            entry
            for entry in hass.config_entries.async_entries(DOMAIN)
            if entry.state is ConfigEntryState.LOADED
            and (entry_id is None or entry.entry_id == entry_id)
        ]
        if not entries:
            raise ServiceValidationError(translation_domain=DOMAIN, translation_key="no_entry")
        errors = []
        for entry in entries:
            manager = entry.runtime_data
            if manager.syncing:
                raise ServiceValidationError(
                    translation_domain=DOMAIN, translation_key="already_syncing"
                )
            await manager.async_sync("service")
            if manager.status.last_result not in OK_RESULTS:
                errors.append(f"{entry.title}: {manager.status.last_error}")
        if errors:
            raise HomeAssistantError("; ".join(errors))

    hass.services.async_register(DOMAIN, SERVICE_SYNC_NOW, _async_sync_now, schema=SYNC_NOW_SCHEMA)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: AtmoHistoryConfigEntry) -> bool:
    """Set up from a config entry."""
    manager = AtmoHistoryManager(hass, entry)
    await manager.async_load()
    entry.runtime_data = manager
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    manager.async_start()
    entry.async_on_unload(manager.async_stop)
    entry.async_on_unload(entry.add_update_listener(_async_reload))
    return True


async def _async_reload(hass: HomeAssistant, entry: AtmoHistoryConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: AtmoHistoryConfigEntry) -> bool:
    """Unload a config entry."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
