"""Config flow for Atmotube history."""

from __future__ import annotations

import re
from typing import Any

import voluptuous as vol
from homeassistant.components.bluetooth import (
    BluetoothServiceInfoBleak,
    async_discovered_service_info,
)
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.const import CONF_ADDRESS
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import (
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from . import influx
from .const import (
    ATMOTUBE_LOCAL_NAME,
    ATMOTUBE_PRO_SERVICE_UUID,
    CONF_ABSENT_MINUTES,
    CONF_CONFIRM_INTERVAL,
    CONF_DRY_RUN,
    CONF_IN_RANGE_MINUTES,
    CONF_INFLUX_BUCKET,
    CONF_INFLUX_ENABLED,
    CONF_INFLUX_ORG,
    CONF_INFLUX_TOKEN,
    CONF_INFLUX_URL,
    CONF_RECORD_INTERVAL,
    CONF_RETRY_MINUTES,
    CONF_SYNC_DELAY,
    DEFAULT_ABSENT_MINUTES,
    DEFAULT_IN_RANGE_MINUTES,
    DEFAULT_RECORD_INTERVAL,
    DEFAULT_RETRY_MINUTES,
    DEFAULT_SYNC_DELAY,
    DOMAIN,
)

MAC_RE = re.compile(r"^[0-9A-F]{2}(:[0-9A-F]{2}){5}$")


def is_atmotube_pro(info: BluetoothServiceInfoBleak) -> bool:
    """Return True for an original Atmotube PRO advertisement."""
    return info.name == ATMOTUBE_LOCAL_NAME and ATMOTUBE_PRO_SERVICE_UUID in [
        uuid.lower() for uuid in info.service_uuids
    ]


def _title(address: str) -> str:
    return f"Atmotube PRO {address[-5:].replace(':', '')}"


class AtmoHistoryConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize."""
        self._discovery: BluetoothServiceInfoBleak | None = None

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        """Return the options flow."""
        return AtmoHistoryOptionsFlow()

    async def async_step_bluetooth(
        self, discovery_info: BluetoothServiceInfoBleak
    ) -> ConfigFlowResult:
        """Handle Bluetooth discovery."""
        if not is_atmotube_pro(discovery_info):
            return self.async_abort(reason="not_supported")
        await self.async_set_unique_id(discovery_info.address.upper())
        self._abort_if_unique_id_configured()
        self._discovery = discovery_info
        self.context["title_placeholders"] = {"name": _title(discovery_info.address)}
        return await self.async_step_bluetooth_confirm()

    async def async_step_bluetooth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Confirm a discovered device."""
        assert self._discovery is not None
        address = self._discovery.address.upper()
        if user_input is not None:
            return self.async_create_entry(title=_title(address), data={CONF_ADDRESS: address})
        self._set_confirm_only()
        return self.async_show_form(
            step_id="bluetooth_confirm",
            description_placeholders={"name": _title(address), "address": address},
        )

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Pick a discovered device or enter a MAC address."""
        errors: dict[str, str] = {}
        if user_input is not None:
            address = user_input[CONF_ADDRESS].strip().upper().replace("-", ":")
            if not MAC_RE.match(address):
                errors[CONF_ADDRESS] = "invalid_address"
            else:
                await self.async_set_unique_id(address)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(title=_title(address), data={CONF_ADDRESS: address})

        configured = self._async_current_ids(include_ignore=False)
        options = [
            SelectOptionDict(value=info.address.upper(), label=f"{info.name} ({info.address})")
            for info in async_discovered_service_info(self.hass, connectable=True)
            if is_atmotube_pro(info) and info.address.upper() not in configured
        ]
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_ADDRESS): SelectSelector(
                        SelectSelectorConfig(options=options, custom_value=True)
                    )
                }
            ),
            errors=errors,
        )


def _minutes(minimum: int, maximum: int) -> NumberSelector:
    return NumberSelector(
        NumberSelectorConfig(
            min=minimum,
            max=maximum,
            step=1,
            unit_of_measurement="min",
            mode=NumberSelectorMode.BOX,
        )
    )


class AtmoHistoryOptionsFlow(OptionsFlow):
    """Options: trigger timing, record interval, dry run and InfluxDB."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Manage the options."""
        errors: dict[str, str] = {}
        if user_input is not None:
            confirm = user_input.pop(CONF_CONFIRM_INTERVAL, False)
            for key in (
                CONF_ABSENT_MINUTES,
                CONF_SYNC_DELAY,
                CONF_RETRY_MINUTES,
                CONF_IN_RANGE_MINUTES,
                CONF_RECORD_INTERVAL,
            ):
                user_input[key] = int(user_input[key])
            if user_input.get(CONF_INFLUX_ENABLED):
                try:
                    await influx.async_test(
                        async_get_clientsession(self.hass),
                        user_input.get(CONF_INFLUX_URL, ""),
                        user_input.get(CONF_INFLUX_TOKEN, ""),
                        user_input.get(CONF_INFLUX_ORG, ""),
                        user_input.get(CONF_INFLUX_BUCKET, ""),
                    )
                except influx.InfluxError:
                    errors["base"] = "influx_failed"
            if not errors:
                if confirm and (manager := getattr(self.config_entry, "runtime_data", None)):
                    await manager.async_confirm_interval(user_input[CONF_RECORD_INTERVAL])
                return self.async_create_entry(data=user_input)

        current = {**self.config_entry.options, **(user_input or {})}

        def opt(key: str, default: Any) -> dict[str, Any]:
            return {"default": current.get(key, default)}

        text = TextSelector()
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_ABSENT_MINUTES, **opt(CONF_ABSENT_MINUTES, DEFAULT_ABSENT_MINUTES)
                ): _minutes(1, 1440),
                vol.Required(
                    CONF_SYNC_DELAY, **opt(CONF_SYNC_DELAY, DEFAULT_SYNC_DELAY)
                ): NumberSelector(
                    NumberSelectorConfig(
                        min=0, max=600, step=1, unit_of_measurement="s", mode=NumberSelectorMode.BOX
                    )
                ),
                vol.Required(
                    CONF_RETRY_MINUTES, **opt(CONF_RETRY_MINUTES, DEFAULT_RETRY_MINUTES)
                ): _minutes(1, 1440),
                vol.Required(
                    CONF_IN_RANGE_MINUTES,
                    **opt(CONF_IN_RANGE_MINUTES, DEFAULT_IN_RANGE_MINUTES),
                ): _minutes(0, 1440),
                vol.Required(
                    CONF_RECORD_INTERVAL, **opt(CONF_RECORD_INTERVAL, DEFAULT_RECORD_INTERVAL)
                ): NumberSelector(
                    NumberSelectorConfig(
                        min=1,
                        max=3600,
                        step=1,
                        unit_of_measurement="s",
                        mode=NumberSelectorMode.BOX,
                    )
                ),
                vol.Required(CONF_CONFIRM_INTERVAL, default=False): bool,
                vol.Required(CONF_DRY_RUN, **opt(CONF_DRY_RUN, False)): bool,
                vol.Required(CONF_INFLUX_ENABLED, **opt(CONF_INFLUX_ENABLED, False)): bool,
                vol.Optional(CONF_INFLUX_URL, **opt(CONF_INFLUX_URL, "")): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.URL)
                ),
                vol.Optional(CONF_INFLUX_TOKEN, **opt(CONF_INFLUX_TOKEN, "")): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.PASSWORD)
                ),
                vol.Optional(CONF_INFLUX_ORG, **opt(CONF_INFLUX_ORG, "")): text,
                vol.Optional(CONF_INFLUX_BUCKET, **opt(CONF_INFLUX_BUCKET, "")): text,
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema, errors=errors)
