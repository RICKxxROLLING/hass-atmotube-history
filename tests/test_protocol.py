"""Tests for the pure protocol module."""

from __future__ import annotations

import time

import pytest

from custom_components.atmo_history.protocol import (
    HistoryBatch,
    NoResponse,
    ProtocolError,
    TransferAborted,
    TransferIncomplete,
    build_hok,
    build_hst,
    decode_record,
    parse_hd,
    parse_ht,
    run_history_transfer,
)

from .fakes import DISCONNECT, FakeAtmotube, hd, ht, record, simple_batch

NOW = int(time.time())
T0 = NOW - 86400


def now() -> int:
    return NOW


class Collector:
    """on_batch handler that records batches."""

    def __init__(self, ack: bool = True, fail: bool = False) -> None:
        self.batches: list[HistoryBatch] = []
        self.ack = ack
        self.fail = fail

    async def __call__(self, batch: HistoryBatch) -> bool:
        if self.fail:
            raise RuntimeError("storage failed")
        self.batches.append(batch)
        return self.ack


def test_commands_big_endian_timestamp() -> None:
    assert build_hst(0x01020304) == b"HST\x01\x02\x03\x04"
    assert build_hok(0x01020304) == b"HOK\x01\x02\x03\x04"


def test_parse_ht() -> None:
    header = parse_ht(ht(T0, 12, 16))
    assert (header.first_timestamp, header.record_count, header.record_size) == (T0, 12, 16)


@pytest.mark.parametrize("data", [b"HT\x00\x01", b"XX\x00\x00\x00\x00\x00\x01\x10"])
def test_parse_ht_malformed(data: bytes) -> None:
    with pytest.raises(ProtocolError):
        parse_ht(data)


def test_parse_ht_record_size_too_small() -> None:
    with pytest.raises(ProtocolError, match="smaller"):
        parse_ht(ht(T0, 1, 12))


def test_parse_hd() -> None:
    packet = parse_hd(hd(7, record()))
    assert packet.total == 7
    assert len(packet.payload) == 16
    with pytest.raises(ProtocolError):
        parse_hd(b"HD\x00")


def test_decode_record_fields() -> None:
    rec = decode_record(record(temp=-5, hum=80, voc=1234, pressure=98765, pad=b"\xab\xcd"), 100)
    assert rec.timestamp == 100
    assert rec.temperature == -5  # int8, negative in a cold car
    assert rec.humidity == 80
    assert rec.voc == 1234
    assert rec.pressure == 98765
    assert (rec.pm1, rec.pm25, rec.pm10) == (3, 5, 7)
    assert rec.extra == b"\xab\xcd"


def test_decode_record_pm_off() -> None:
    rec = decode_record(record(pm1=0xFFFF, pm25=0xFFFF, pm10=0xFFFF), 0)
    assert rec.pm1 is rec.pm25 is rec.pm10 is None
    assert set(rec.values()) == {"temperature", "humidity", "voc", "pressure"}


def test_decode_record_no_reading_markers() -> None:
    rec = decode_record(b"\x80\x80\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00", 0)
    assert rec.temperature is rec.humidity is rec.voc is rec.pressure is None
    assert rec.values() == {}


def test_decode_record_14_bytes() -> None:
    rec = decode_record(record(pad=b""), 0)
    assert rec.extra == b""


async def test_single_batch() -> None:
    device = FakeAtmotube([simple_batch(T0, [20, 21, 22])])
    handler = Collector()
    result = await run_history_transfer(device, handler, now)

    assert device.writes[0] == build_hst(NOW)
    assert device.ack_writes == [build_hok(NOW)]
    assert (result.batches_acked, result.records_acked) == (1, 3)
    records = handler.batches[0].records(60)
    assert [r.timestamp for r in records] == [T0, T0 + 60, T0 + 120]
    assert [r.temperature for r in records] == [20, 21, 22]


async def test_multi_record_packets() -> None:
    packets = [
        ht(T0, 4, 16),
        hd(2, record(temp=1), record(temp=2)),
        hd(4, record(temp=3), record(temp=4)),
    ]
    handler = Collector()
    result = await run_history_transfer(FakeAtmotube([packets]), handler, now)
    assert result.records_acked == 4
    assert [r.temperature for r in handler.batches[0].records(60)] == [1, 2, 3, 4]


async def test_multiple_batches_loop_until_silence() -> None:
    device = FakeAtmotube(
        [
            simple_batch(T0, [1, 2]),
            simple_batch(T0 + 120, [3, 4, 5]),
            simple_batch(T0 + 300, [6]),
        ]
    )
    handler = Collector()
    result = await run_history_transfer(device, handler, now)
    assert (result.batches_acked, result.records_acked) == (3, 6)
    assert device.acks == 3
    assert not device.batches


async def test_empty_history() -> None:
    device = FakeAtmotube([])
    handler = Collector()
    result = await run_history_transfer(device, handler, now)
    assert result.batches_acked == 0
    assert device.writes == [build_hst(NOW)]
    assert not handler.batches


async def test_ht_without_hok_is_accepted() -> None:
    device = FakeAtmotube([simple_batch(T0, [1])], reply_hok=False)
    result = await run_history_transfer(device, Collector(), now)
    assert result.batches_acked == 1


async def test_no_response() -> None:
    with pytest.raises(NoResponse):
        await run_history_transfer(FakeAtmotube([], respond=False), Collector(), now)


async def test_truncated_transfer_is_not_acked() -> None:
    packets = simple_batch(T0, [1, 2, 3])[:-1]  # last HD missing
    device = FakeAtmotube([packets])
    handler = Collector()
    with pytest.raises(TransferIncomplete, match="2 of 3"):
        await run_history_transfer(device, handler, now)
    assert not handler.batches
    assert device.ack_writes == []


async def test_truncated_second_batch_keeps_first() -> None:
    device = FakeAtmotube([simple_batch(T0, [1]), simple_batch(T0 + 60, [2, 3])[:-1]])
    handler = Collector()
    with pytest.raises(TransferIncomplete):
        await run_history_transfer(device, handler, now)
    assert device.acks == 1
    assert len(handler.batches) == 1


async def test_disconnect_mid_transfer() -> None:
    packets = simple_batch(T0, [1, 2, 3])
    device = FakeAtmotube([[*packets[:2], DISCONNECT]])
    handler = Collector()
    with pytest.raises(TransferAborted):
        await run_history_transfer(device, handler, now)
    assert not handler.batches
    assert device.ack_writes == []


async def test_new_header_mid_batch() -> None:
    packets = [*simple_batch(T0, [1, 2])[:2], ht(T0 + 600, 1)]
    with pytest.raises(TransferIncomplete, match="New HT"):
        await run_history_transfer(FakeAtmotube([packets]), Collector(), now)


async def test_storage_failure_is_not_acked() -> None:
    device = FakeAtmotube([simple_batch(T0, [1])])
    with pytest.raises(RuntimeError):
        await run_history_transfer(device, Collector(fail=True), now)
    assert device.ack_writes == []


async def test_handler_can_stop_without_ack() -> None:
    device = FakeAtmotube([simple_batch(T0, [1]), simple_batch(T0 + 60, [2])])
    result = await run_history_transfer(device, Collector(ack=False), now)
    assert result.stopped_early
    assert device.ack_writes == []


async def test_hd_before_ht_ignored() -> None:
    device = FakeAtmotube([[hd(1, record()), *simple_batch(T0, [1])]])
    result = await run_history_transfer(device, Collector(), now)
    assert result.records_acked == 1


@pytest.mark.parametrize(
    ("packets", "match"),
    [
        ([ht(T0, 2), hd(1, record()), hd(1, record())], "does not match"),
        ([ht(T0, 3), hd(1, record()), hd(3, record())], "does not match"),
        ([ht(T0, 2), hd(3, record(), record(), record())], "exceeds"),
        ([ht(T0, 1), hd(1, record() + b"\x00")], "multiple"),
    ],
)
async def test_inconsistent_packets(packets: list[bytes], match: str) -> None:
    device = FakeAtmotube([packets])
    with pytest.raises(ProtocolError, match=match):
        await run_history_transfer(device, Collector(), now)
    assert device.ack_writes == []


async def test_little_endian_timestamp_is_rejected_with_hint() -> None:
    # Low byte 0 so the byte-swapped value is far below 2016.
    first = T0 & ~0xFF
    device = FakeAtmotube([[ht(first, 1, order="little"), hd(1, record())]])
    with pytest.raises(ProtocolError, match="byte order is probably wrong"):
        await run_history_transfer(device, Collector(), now)
    assert device.ack_writes == []


async def test_empty_ht_batch_is_acked() -> None:
    device = FakeAtmotube([[ht(T0, 0)]])
    handler = Collector()
    result = await run_history_transfer(device, handler, now)
    assert result.batches_acked == 1
    assert handler.batches[0].records(60) == []


@pytest.mark.parametrize("module", ["protocol.py", "aggregate.py"])
def test_pure_modules_have_no_ha_or_bleak_imports(module: str) -> None:
    import ast
    from pathlib import Path

    path = Path(__file__).parent.parent / "custom_components" / "atmo_history" / module
    names: list[str] = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            names += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            names.append(node.module or "")
    assert not [
        n for n in names if n.split(".")[0] in ("homeassistant", "bleak", "bleak_retry_connector")
    ]


# Captured from a real Atmotube PRO through an ESPHome proxy (2026-10-05).
CAPTURE_HST_TIME = 0x6AC3C645
CAPTURE = [
    bytes.fromhex("4854006ac3bd992510"),
    bytes.fromhex(
        "4844000f8080ffff618e010001000100030000008080ffff5c8e010001000100020000008080"
        "ffff5a8e010001000100020000008080ffff578e010001000100020000008080ffff5a8e0100"
        "010001000200000080800700598e0100010001000200000080800d00538e01000100010002000000"
        "80801100518e0100010001000200000080801500508e01000100010002000000808017004e8e0100"
        "0100010003000000808019004a8e0100010002000300000080801a00478e01000100030004000000"
        "80801a00458e0100020003000500000080801900468e01000100030005000000808017004a8e0100"
        "0100030005000000"
    ),
    bytes.fromhex(
        "4844001e808015004e8e0100010003000400000080801400538e0100010003000400000080801300"
        "528e0100010002000400000080801200508e0100010002000400000080801100528e010001000300"
        "0400000080801000508e0100010002000400000080800f004c8e0100010002000400000080800e00"
        "4c8e0100010002000400000080800d00498e0100010002000300000080800c00478e010001000200"
        "0300000080800b00478e0100010002000300000080800a004a8e0100010002000300000080800900"
        "498e0100010002000300000080800800488e01000100020003000000808007004b8e010001000200"
        "03000000"
    ),
    bytes.fromhex(
        "4844002580800600478e0100010001000300000080800500448e0100010001000300000080800400"
        "478e0100010002000300000080800300488e0100010002000300000080800200488e010001000200"
        "0300000080800100438e01000100010003000000808001003e8e01000100010003000000"
    ),
]


async def test_real_capture() -> None:
    device = FakeAtmotube([CAPTURE])
    handler = Collector()
    result = await run_history_transfer(device, handler, lambda: CAPTURE_HST_TIME)
    assert result.records_acked == 37
    assert device.acks == 1

    header = handler.batches[0].header
    assert (header.first_timestamp, header.record_count, header.record_size) == (
        1791212953,
        37,
        16,
    )
    # 37 records at 60 s end exactly at the HST time.
    assert header.first_timestamp + 37 * 60 == CAPTURE_HST_TIME

    records = handler.batches[0].records(60)
    first, last = records[0], records[-1]
    assert first.temperature is None and first.humidity is None  # 0x80 markers
    assert first.voc is None  # 0xFFFF while the VOC sensor had no reading
    assert (first.pressure, first.pm1, first.pm25, first.pm10) == (101985, 1, 1, 3)
    assert first.extra == b"\x00\x00"
    assert records[5].voc == 7
    assert (last.timestamp, last.voc, last.pressure) == (1791212953 + 36 * 60, 1, 101950)
