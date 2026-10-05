"""Decode the Atmotube PRO's Bluetooth advertisement (no connection needed).

Layout of the manufacturer data (company ID 0xFFFF), as decoded by the
ha-atmo integration (MIT, Nathan Spencer) and Atmotube's Android library:

    0-1  VOC (big-endian)      4  humidity        10  info byte
    2-3  device id             5  temperature     11  battery %
                             6-9  pressure (big-endian)

Info byte bits: 0 PM sensor on, 1 error, 2 bonded, 3 charging, 4 timer,
6 VOC ready.

No Home Assistant imports.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

MANUFACTURER_ID = 0xFFFF
MIN_LENGTH = 12
CHARGING_BIT = 3


@dataclass(frozen=True, slots=True)
class AdvertisementStatus:
    """Device status carried in every advertisement."""

    battery: int  # %
    charging: bool


def parse_advertisement(manufacturer_data: Mapping[int, bytes]) -> AdvertisementStatus | None:
    """Return battery and charging state, or None if not an Atmotube PRO broadcast."""
    data = manufacturer_data.get(MANUFACTURER_ID)
    if data is None or len(data) < MIN_LENGTH:
        return None
    battery = data[11]
    if battery > 100:
        return None
    return AdvertisementStatus(battery=battery, charging=bool(data[10] >> CHARGING_BIT & 1))
