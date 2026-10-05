"""Config and options flow tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from bleak.backends.device import BLEDevice
from homeassistant.components.bluetooth import BluetoothServiceInfoBleak
from homeassistant.config_entries import SOURCE_BLUETOOTH, SOURCE_USER
from homeassistant.const import CONF_ADDRESS
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.atmo_history.const import (
    ATMOTUBE_PRO_SERVICE_UUID,
    CONF_CONFIRM_INTERVAL,
    CONF_INFLUX_ENABLED,
    CONF_RECORD_INTERVAL,
    DOMAIN,
)
from custom_components.atmo_history.influx import InfluxError

ADDRESS = "AA:BB:CC:DD:EE:FF"


def service_info(
    name: str = "ATMOTUBE", uuids: list[str] | None = None
) -> BluetoothServiceInfoBleak:
    return BluetoothServiceInfoBleak(
        name=name,
        address=ADDRESS,
        rssi=-60,
        manufacturer_data={},
        service_data={},
        service_uuids=uuids if uuids is not None else [ATMOTUBE_PRO_SERVICE_UUID],
        source="esphome_proxy",
        device=BLEDevice(ADDRESS, name, None),
        advertisement=None,
        connectable=True,
        time=0,
        tx_power=None,
    )


@pytest.fixture(autouse=True)
def mock_setup_entry():
    with patch("custom_components.atmo_history.async_setup_entry", return_value=True) as mock:
        yield mock


async def test_bluetooth_discovery(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_BLUETOOTH}, data=service_info()
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "bluetooth_confirm"
    result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == {CONF_ADDRESS: ADDRESS}
    assert result["result"].unique_id == ADDRESS
    assert result["title"] == "Atmotube PRO EEFF"


async def test_bluetooth_discovery_not_pro(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_BLUETOOTH},
        data=service_info(uuids=["db450001-8e9a-4818-add7-6ed94a328ab3"]),
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "not_supported"


async def test_bluetooth_discovery_already_configured(hass: HomeAssistant) -> None:
    MockConfigEntry(domain=DOMAIN, unique_id=ADDRESS, data={CONF_ADDRESS: ADDRESS}).add_to_hass(
        hass
    )
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_BLUETOOTH}, data=service_info()
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


async def test_user_manual_mac(hass: HomeAssistant) -> None:
    with patch(
        "custom_components.atmo_history.config_flow.async_discovered_service_info",
        return_value=[],
    ):
        result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_USER})
        assert result["type"] is FlowResultType.FORM
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_ADDRESS: "not-a-mac"}
        )
        assert result["errors"] == {CONF_ADDRESS: "invalid_address"}
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_ADDRESS: "aa-bb-cc-dd-ee-ff"}
        )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == {CONF_ADDRESS: ADDRESS}


async def test_user_lists_discovered(hass: HomeAssistant) -> None:
    with patch(
        "custom_components.atmo_history.config_flow.async_discovered_service_info",
        return_value=[service_info(), service_info(name="OTHER")],
    ):
        result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_USER})
    selector = result["data_schema"].schema[CONF_ADDRESS]
    assert [o["value"] for o in selector.config["options"]] == [ADDRESS]


async def test_options_flow(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(domain=DOMAIN, unique_id=ADDRESS, data={CONF_ADDRESS: ADDRESS})
    entry.add_to_hass(hass)
    manager = AsyncMock()
    entry.runtime_data = manager

    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] is FlowResultType.FORM
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {
            "absent_minutes": 20,
            "sync_delay": 30,
            "retry_minutes": 5,
            "in_range_minutes": 10,
            CONF_RECORD_INTERVAL: 300,
            CONF_CONFIRM_INTERVAL: True,
            "dry_run": False,
            CONF_INFLUX_ENABLED: False,
        },
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options["absent_minutes"] == 20
    assert entry.options["in_range_minutes"] == 10
    assert entry.options[CONF_RECORD_INTERVAL] == 300
    assert CONF_CONFIRM_INTERVAL not in entry.options
    manager.async_confirm_interval.assert_awaited_once_with(300)


async def test_options_flow_influx_failure(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(domain=DOMAIN, unique_id=ADDRESS, data={CONF_ADDRESS: ADDRESS})
    entry.add_to_hass(hass)
    result = await hass.config_entries.options.async_init(entry.entry_id)
    with patch(
        "custom_components.atmo_history.config_flow.influx.async_test",
        side_effect=InfluxError("nope"),
    ):
        result = await hass.config_entries.options.async_configure(
            result["flow_id"],
            {
                "absent_minutes": 10,
                "sync_delay": 30,
                "retry_minutes": 15,
                CONF_RECORD_INTERVAL: 60,
                CONF_CONFIRM_INTERVAL: False,
                "dry_run": False,
                CONF_INFLUX_ENABLED: True,
                "influx_url": "http://influx:8086",
                "influx_token": "t",
                "influx_org": "o",
                "influx_bucket": "b",
            },
        )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "influx_failed"}
