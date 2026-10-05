"""Minimal InfluxDB v2 line-protocol writer."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import aiohttp

from .protocol import HistoryRecord

MEASUREMENT = "atmotube_history"
CHUNK_SIZE = 5000
TIMEOUT = aiohttp.ClientTimeout(total=30)


class InfluxError(Exception):
    """Writing to InfluxDB failed."""


def _escape_tag(value: str) -> str:
    for char in ("\\", ",", "=", " "):
        value = value.replace(char, "\\" + char)
    return value


def to_line_protocol(address: str, records: Iterable[HistoryRecord]) -> list[str]:
    """Render records as line protocol with second precision."""
    tag = _escape_tag(address)
    lines = []
    for record in records:
        if not (values := record.values()):
            continue
        fields = ",".join(f"{k}={float(v)}" for k, v in values.items())
        lines.append(f"{MEASUREMENT},device={tag} {fields} {record.timestamp}")
    return lines


async def async_write(
    session: aiohttp.ClientSession,
    url: str,
    token: str,
    org: str,
    bucket: str,
    lines: Sequence[str],
) -> None:
    """Write lines, raising InfluxError on any failure."""
    endpoint = f"{url.rstrip('/')}/api/v2/write"
    params = {"org": org, "bucket": bucket, "precision": "s"}
    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "text/plain; charset=utf-8",
    }
    for start in range(0, len(lines), CHUNK_SIZE):
        body = "\n".join(lines[start : start + CHUNK_SIZE])
        try:
            async with session.post(
                endpoint, params=params, headers=headers, data=body, timeout=TIMEOUT
            ) as resp:
                if resp.status != 204:
                    text = await resp.text()
                    raise InfluxError(f"HTTP {resp.status}: {text[:200]}")
        except (aiohttp.ClientError, TimeoutError) as err:
            raise InfluxError(str(err) or type(err).__name__) from err


async def async_test(
    session: aiohttp.ClientSession, url: str, token: str, org: str, bucket: str
) -> None:
    """Check that the URL, token, org and bucket resolve."""
    endpoint = f"{url.rstrip('/')}/api/v2/buckets"
    try:
        async with session.get(
            endpoint,
            params={"name": bucket, "org": org},
            headers={"Authorization": f"Token {token}"},
            timeout=TIMEOUT,
        ) as resp:
            if resp.status != 200:
                raise InfluxError(f"HTTP {resp.status}: {(await resp.text())[:200]}")
            data = await resp.json()
    except (aiohttp.ClientError, TimeoutError) as err:
        raise InfluxError(str(err) or type(err).__name__) from err
    if not data.get("buckets"):
        raise InfluxError(f"Bucket {bucket!r} not found")
