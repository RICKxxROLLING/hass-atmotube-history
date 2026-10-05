"""Atmotube PRO history protocol over the Nordic UART service.

This module has no Home Assistant or bleak imports so it can be tested in
isolation. The BLE transport is abstracted behind :class:`Transport`.

Flow (Atmotube "Bluetooth API" article, confirmed against the atmotuber
Flutter package):

1. Write ``HST`` + uint32 Unix time. The device answers ``HOK``.
2. If it has unsynced history it sends ``HT``:
   ``"HT", 0x00, uint32 first-record time, uint8 record count, uint8 record size``
3. Then ``HD`` packets until that many records have arrived:
   ``"HD", 0x00, uint8 running record total, records...``
   Atmotube's article calls these "number of HD packets" and "packet number",
   but a real capture (MTU-sized packets of 15 records each) shows a record
   count and a running record total, e.g. HT count 37 then HD 15, 30, 37.
   atmotuber's ``diff = packetNumber - previousPacketNumber`` agrees.
4. After a complete batch, write ``HOK`` + uint32 Unix time. The device marks
   the batch as synced and sends the next ``HT`` if there is more.
   There is no end-of-history packet; silence means done.

Things that are NOT documented by Atmotube and are handled defensively:

* Timestamp byte order: big-endian (atmotuber, confirmed by a capture whose
  first-record time plus 37 x 60 s equalled the HST time). Record fields are
  little-endian. Implausible header times abort the transfer without an ACK.
* The interval between records (atmotuber and the capture: 60 s). The caller
  supplies it and verifies it.
* Record size: 16 on the wire; the documented 14-byte layout is followed by
  two bytes kept in ``HistoryRecord.extra``.
* "No reading" markers: 0x80 for temperature and humidity, 0xFFFF for VOC
  and PM, 0xFFFFFFFF for pressure.
"""

from __future__ import annotations

import logging
import struct
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

_LOGGER = logging.getLogger(__name__)

UART_SERVICE_UUID = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
# Named from the device's point of view, as in Atmotube's Android library.
UART_RX_CHAR_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"  # we write here
UART_TX_CHAR_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"  # we get notified here

TIMESTAMP_BYTEORDER = "big"

RECORD_STRUCT = struct.Struct("<bBHIHHH")
RECORD_MIN_SIZE = RECORD_STRUCT.size  # 14
TEMPERATURE_NOT_AVAILABLE = -128  # 0x80
HUMIDITY_MAX = 100  # 0x80 seen when not available
U16_NOT_AVAILABLE = 0xFFFF
U32_NOT_AVAILABLE = 0xFFFFFFFF

HT_LENGTH = 9
HD_HEADER_LENGTH = 4

# 2016-01-01T00:00:00Z, before any Atmotube PRO shipped.
MIN_PLAUSIBLE_TIMESTAMP = 1451606400
MAX_FUTURE_SKEW = 86400

KIND_HOK = "HOK"
KIND_HT = "HT"
KIND_HD = "HD"


class ProtocolError(Exception):
    """The device sent something we cannot safely interpret."""


class TransferIncomplete(ProtocolError):
    """A batch was started but not finished."""


class NoResponse(ProtocolError):
    """The device never answered the HST request."""


class TransferAborted(Exception):
    """The transport went away (e.g. BLE disconnect)."""


def encode_timestamp(timestamp: int) -> bytes:
    """Encode a Unix time the way the device expects it."""
    return int(timestamp).to_bytes(4, TIMESTAMP_BYTEORDER, signed=False)


def decode_timestamp(data: bytes) -> int:
    """Decode a 4-byte Unix time from the device."""
    return int.from_bytes(data, TIMESTAMP_BYTEORDER, signed=False)


def build_hst(now: int) -> bytes:
    """Build the history request command."""
    return b"HST" + encode_timestamp(now)


def build_hok(now: int) -> bytes:
    """Build the batch acknowledgement command."""
    return b"HOK" + encode_timestamp(now)


def packet_kind(data: bytes) -> str | None:
    """Classify a notification from the TX characteristic."""
    if data.startswith(b"HO"):
        return KIND_HOK
    if data.startswith(b"HT"):
        return KIND_HT
    if data.startswith(b"HD"):
        return KIND_HD
    return None


@dataclass(frozen=True, slots=True)
class HistoryHeader:
    """Parsed HT packet."""

    first_timestamp: int
    record_count: int
    record_size: int


@dataclass(frozen=True, slots=True)
class HistoryDataPacket:
    """Parsed HD packet."""

    total: int  # running record total including this packet
    payload: bytes


@dataclass(frozen=True, slots=True)
class HistoryRecord:
    """One decoded measurement."""

    timestamp: int
    # None wherever the device stored a "no reading" marker.
    temperature: int | None  # °C
    humidity: int | None  # %
    voc: int | None  # ppb
    pressure: int | None  # Pa (the device sends mbar * 100)
    pm1: int | None  # µg/m³
    pm25: int | None
    pm10: int | None
    extra: bytes = b""

    def values(self) -> dict[str, float]:
        """Return the metrics that have a value, keyed by metric name."""
        out: dict[str, float] = {}
        for key in ("temperature", "humidity", "voc", "pressure", "pm1", "pm25", "pm10"):
            if (value := getattr(self, key)) is not None:
                out[key] = value
        return out


def parse_ht(data: bytes) -> HistoryHeader:
    """Parse an HT packet."""
    if len(data) < HT_LENGTH or not data.startswith(b"HT"):
        raise ProtocolError(f"Malformed HT packet: {data.hex()}")
    header = HistoryHeader(
        first_timestamp=decode_timestamp(data[3:7]),
        record_count=data[7],
        record_size=data[8],
    )
    if header.record_size < RECORD_MIN_SIZE:
        raise ProtocolError(
            f"HT record size {header.record_size} is smaller than the "
            f"documented {RECORD_MIN_SIZE}-byte layout"
        )
    return header


def parse_hd(data: bytes) -> HistoryDataPacket:
    """Parse an HD packet."""
    if len(data) < HD_HEADER_LENGTH or not data.startswith(b"HD"):
        raise ProtocolError(f"Malformed HD packet: {data.hex()}")
    return HistoryDataPacket(total=data[3], payload=bytes(data[4:]))


def check_timestamp_plausible(timestamp: int, now: int) -> None:
    """Refuse header times that point at a byte-order or clock problem."""
    if MIN_PLAUSIBLE_TIMESTAMP <= timestamp <= now + MAX_FUTURE_SKEW:
        return
    raw = timestamp.to_bytes(4, TIMESTAMP_BYTEORDER)
    other = "little" if TIMESTAMP_BYTEORDER == "big" else "big"
    alternative = int.from_bytes(raw, other)
    hint = ""
    if MIN_PLAUSIBLE_TIMESTAMP <= alternative <= now + MAX_FUTURE_SKEW:
        hint = (
            f"; read as {other}-endian it would be {alternative}, which is "
            "plausible, so the timestamp byte order is probably wrong"
        )
    raise ProtocolError(
        f"Implausible first-record time {timestamp} ({raw.hex()}) decoded as "
        f"{TIMESTAMP_BYTEORDER}-endian{hint}"
    )


def decode_record(chunk: bytes, timestamp: int) -> HistoryRecord:
    """Decode one record. ``chunk`` may be longer than the known layout."""
    temp, hum, voc, pressure, pm1, pm25, pm10 = RECORD_STRUCT.unpack_from(chunk)

    def u16(value: int) -> int | None:
        return None if value == U16_NOT_AVAILABLE else value

    return HistoryRecord(
        timestamp=timestamp,
        temperature=None if temp == TEMPERATURE_NOT_AVAILABLE else temp,
        humidity=None if hum > HUMIDITY_MAX else hum,
        voc=u16(voc),
        pressure=None if pressure == U32_NOT_AVAILABLE else pressure,
        pm1=u16(pm1),
        pm25=u16(pm25),
        pm10=u16(pm10),
        extra=bytes(chunk[RECORD_MIN_SIZE:]),
    )


@dataclass
class HistoryBatch:
    """One HT header plus its HD packets."""

    header: HistoryHeader
    packets: list[bytes] = field(default_factory=list)
    record_count: int = 0

    def add(self, packet: HistoryDataPacket) -> None:
        """Add an HD packet, rejecting anything inconsistent."""
        size = self.header.record_size
        if len(packet.payload) % size:
            raise ProtocolError(
                f"HD payload of {len(packet.payload)} bytes is not a multiple "
                f"of the record size {size}"
            )
        expected = self.record_count + len(packet.payload) // size
        if packet.total != expected:
            raise ProtocolError(
                f"HD running total {packet.total} does not match the "
                f"{expected} records received (packet lost or repeated)"
            )
        if expected > self.header.record_count:
            raise ProtocolError(
                f"HD running total {expected} exceeds the "
                f"{self.header.record_count} records announced in HT"
            )
        self.packets.append(packet.payload)
        self.record_count = expected

    @property
    def complete(self) -> bool:
        """Return True once every announced record has arrived."""
        return self.record_count == self.header.record_count

    def payload(self) -> bytes:
        """Concatenate packet payloads in arrival order."""
        return b"".join(self.packets)

    def records(self, interval: int) -> list[HistoryRecord]:
        """Decode all records, timestamped ``interval`` seconds apart."""
        if not self.complete:
            raise TransferIncomplete("Batch is not complete")
        data = self.payload()
        size = self.header.record_size
        first = self.header.first_timestamp
        return [
            decode_record(data[offset : offset + size], first + index * interval)
            for index, offset in enumerate(range(0, len(data), size))
        ]


class Transport(Protocol):
    """Minimal async UART transport."""

    async def write(self, data: bytes) -> None:
        """Write a command to the device."""

    async def receive(self, timeout: float) -> bytes:
        """Return the next notification.

        Raises TimeoutError when nothing arrives in time and
        TransferAborted when the link is lost.
        """


@dataclass(frozen=True, slots=True)
class Timeouts:
    """Per-step timeouts in seconds."""

    response: float = 10.0  # HST -> first reply
    header: float = 15.0  # waiting for the next HT
    packet: float = 10.0  # gap between HD packets


DEFAULT_TIMEOUTS = Timeouts()


@dataclass(slots=True)
class TransferResult:
    """Outcome of a transfer."""

    batches_acked: int = 0
    records_acked: int = 0
    stopped_early: bool = False


BatchHandler = Callable[[HistoryBatch], Awaitable[bool]]


async def run_history_transfer(
    transport: Transport,
    on_batch: BatchHandler,
    now: Callable[[], int],
    timeouts: Timeouts = DEFAULT_TIMEOUTS,
    max_batches: int = 1000,
) -> TransferResult:
    """Download history, calling ``on_batch`` for every complete batch.

    ``on_batch`` must persist the batch and return True before we ACK it.
    Returning False stops the transfer without an ACK. Raising aborts it
    without an ACK. Partial batches are never passed to ``on_batch``.
    """
    result = TransferResult()
    got_reply = False
    batch: HistoryBatch | None = None

    await transport.write(build_hst(now()))

    while True:
        if batch is not None:
            timeout = timeouts.packet
        elif got_reply:
            timeout = timeouts.header
        else:
            timeout = timeouts.response
        try:
            data = await transport.receive(timeout)
        except TimeoutError:
            if batch is not None:
                raise TransferIncomplete(
                    f"Timed out after {batch.record_count} of {batch.header.record_count} records"
                ) from None
            if not got_reply:
                raise NoResponse("No reply to HST") from None
            return result

        kind = packet_kind(data)
        _LOGGER.debug("RX %s: %s", kind or "?", data.hex())

        if kind == KIND_HOK:
            got_reply = True
            continue
        if kind == KIND_HT:
            got_reply = True
            if batch is not None:
                raise TransferIncomplete(
                    f"New HT after {batch.record_count} of {batch.header.record_count} records"
                )
            header = parse_ht(data)
            check_timestamp_plausible(header.first_timestamp, now())
            _LOGGER.debug(
                "HT: first=%s records=%s record_size=%s",
                header.first_timestamp,
                header.record_count,
                header.record_size,
            )
            batch = HistoryBatch(header)
        elif kind == KIND_HD:
            if batch is None:
                _LOGGER.debug("Ignoring HD packet received before HT")
                continue
            batch.add(parse_hd(data))
        else:
            _LOGGER.debug("Ignoring unknown packet")
            continue

        if batch is None or not batch.complete:
            continue

        if not await on_batch(batch):
            result.stopped_early = True
            return result
        await transport.write(build_hok(now()))
        _LOGGER.debug("ACKed batch starting %s", batch.header.first_timestamp)
        result.batches_acked += 1
        result.records_acked += batch.record_count
        batch = None
        if result.batches_acked >= max_batches:
            return result
