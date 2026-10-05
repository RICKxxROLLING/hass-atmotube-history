"""Tests for advertisement decoding."""

from __future__ import annotations

from custom_components.atmo_history.advertisement import (
    AdvertisementStatus,
    parse_advertisement,
)

# Captured from the real device through an ESPHome proxy (2026-10-05).
CAPTURED = bytes.fromhex("005717ac3819") + bytes.fromhex("00018dbd4064")


def test_captured_advertisement() -> None:
    assert parse_advertisement({0xFFFF: CAPTURED}) == AdvertisementStatus(100, False)


def test_charging_bit() -> None:
    data = bytearray(CAPTURED)
    data[10] |= 0b1000
    data[11] = 78
    assert parse_advertisement({0xFFFF: bytes(data)}) == AdvertisementStatus(78, True)


def test_not_an_atmotube_advertisement() -> None:
    assert parse_advertisement({}) is None
    assert parse_advertisement({0x004C: CAPTURED}) is None
    assert parse_advertisement({0xFFFF: CAPTURED[:8]}) is None
    assert parse_advertisement({0xFFFF: CAPTURED[:11] + b"\xc8"}) is None  # 200 %
