"""Sync orchestration: triggers, locking, persistence and import."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import TYPE_CHECKING, Any

from bleak.exc import BleakError
from homeassistant.components import bluetooth
from homeassistant.components.recorder import DOMAIN as RECORDER_DOMAIN
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import async_import_statistics
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HassJob, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from . import influx
from .advertisement import AdvertisementStatus, parse_advertisement
from .aggregate import (
    INTERVAL_GAP,
    INTERVAL_MATCH,
    INTERVAL_MISMATCH,
    HourlyAggregator,
    batch_ends_at,
    check_continuity,
    is_resend,
)
from .ble import async_download_history
from .const import (
    CONF_ABSENT_MINUTES,
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
    ISSUE_INTERVAL_MISMATCH,
    RESULT_AWAITING_INTERVAL,
    RESULT_DRY_RUN,
    RESULT_ERROR,
    RESULT_INTERVAL_MISMATCH,
    RESULT_NO_DATA,
    RESULT_SUCCESS,
    RESULT_UNAVAILABLE,
    SESSION_TIMEOUT,
    SIGNAL_STATUS,
    SIGNAL_UPDATED,
)
from .history import async_backfill_states
from .protocol import (
    HistoryBatch,
    HistoryDataPacket,
    HistoryHeader,
    HistoryRecord,
    ProtocolError,
    TransferAborted,
)
from .statistics import METRIC_META, async_import_records

if TYPE_CHECKING:
    from .sensor import AtmoHistoryValueSensor

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 1

type AtmoHistoryConfigEntry = ConfigEntry[AtmoHistoryManager]


class IntervalMismatch(Exception):
    """The measured record interval does not match the configured one."""

    def __init__(self, measured: float, configured: int) -> None:
        """Initialize."""
        super().__init__(
            f"Measured record interval {measured:.1f} s does not match the "
            f"configured {configured} s"
        )
        self.measured = measured
        self.configured = configured


@dataclass
class SyncStatus:
    """What the diagnostic sensors show."""

    last_success: datetime | None = None
    last_attempt: datetime | None = None
    last_records: int | None = None
    last_result: str | None = None
    last_error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Serialize."""
        data = asdict(self)
        for key in ("last_success", "last_attempt"):
            if data[key] is not None:
                data[key] = data[key].isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> SyncStatus:
        """Deserialize."""
        status = cls(**(data or {}))
        for key in ("last_success", "last_attempt"):
            if (value := getattr(status, key)) is not None:
                setattr(status, key, dt_util.parse_datetime(value))
        return status


def _batch_from_dict(data: dict[str, Any]) -> HistoryBatch:
    batch = HistoryBatch(
        HistoryHeader(
            first_timestamp=data["first_timestamp"],
            record_count=data["record_count"],
            record_size=data["record_size"],
        )
    )
    payload = bytes.fromhex(data["payload"])
    batch.add(HistoryDataPacket(len(payload) // data["record_size"], payload))
    return batch


class AtmoHistoryManager:
    """Owns the sync lifecycle for one Atmotube."""

    def __init__(self, hass: HomeAssistant, entry: AtmoHistoryConfigEntry) -> None:
        """Initialize."""
        self.hass = hass
        self.entry = entry
        self.address: str = entry.unique_id or entry.data["address"]
        self.name = entry.title
        self.status = SyncStatus()
        # Small store written before every ACK; the device resends a batch
        # if it is not acknowledged within about 5 s.
        self._store: Store[dict[str, Any]] = Store(
            hass, STORAGE_VERSION, f"{DOMAIN}.{entry.entry_id}"
        )
        self._hours_store: Store[dict[str, Any]] = Store(
            hass, STORAGE_VERSION, f"{DOMAIN}.{entry.entry_id}.hours"
        )
        self._aggregator = HourlyAggregator()
        self._confirmed_interval: int | None = None
        self._last_batch: dict[str, Any] | None = None
        self._pending: list[dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._last_seen: float | None = None
        self._last_attempt: float | None = None
        self._failed = False
        self._blocked = False
        self._cancel_scheduled: CALLBACK_TYPE | None = None
        self._session_imported = 0
        self._session_decoded = 0
        self._session_started: int | None = None
        self._session_resends = 0
        self._unsubs: list[Callable[[], None]] = []
        # Battery and charging state from the latest advertisement
        self.device_status: AdvertisementStatus | None = None
        # metric -> history value sensor, filled in by the sensor platform
        self.history_entities: dict[str, AtmoHistoryValueSensor] = {}

    # Options -----------------------------------------------------------

    def _opt(self, key: str, default: Any) -> Any:
        return self.entry.options.get(key, default)

    @property
    def interval(self) -> int:
        """Configured record interval in seconds."""
        return int(self._opt(CONF_RECORD_INTERVAL, DEFAULT_RECORD_INTERVAL))

    @property
    def interval_confirmed(self) -> bool:
        """Whether the configured interval has been verified."""
        return self._confirmed_interval == self.interval

    @property
    def pending_batches(self) -> int:
        """Batches downloaded and ACKed but not yet imported."""
        return len(self._pending)

    @property
    def syncing(self) -> bool:
        """Whether a sync is running."""
        return self._lock.locked()

    # Lifecycle ---------------------------------------------------------

    async def async_load(self) -> None:
        """Load persisted state."""
        data = await self._store.async_load() or {}
        hours = await self._hours_store.async_load()
        # Version 0.1.1 kept the hours in the main store.
        self._aggregator = HourlyAggregator.from_dict(
            hours.get("hours") if hours else data.get("hours")
        )
        self._confirmed_interval = data.get("confirmed_interval")
        self._last_batch = data.get("last_batch")
        self._pending = data.get("pending", [])
        self.status = SyncStatus.from_dict(data.get("status"))

    async def _async_save(self) -> None:
        await self._hours_store.async_save({"hours": self._aggregator.as_dict()})
        await self._async_save_sync_state()

    async def _async_save_sync_state(self) -> None:
        await self._store.async_save(
            {
                "confirmed_interval": self._confirmed_interval,
                "last_batch": self._last_batch,
                "pending": self._pending,
                "status": self.status.as_dict(),
            }
        )

    @callback
    def async_start(self) -> None:
        """Start listening for the device."""
        ir.async_delete_issue(self.hass, DOMAIN, self._issue_id)
        self._unsubs.append(
            bluetooth.async_register_callback(
                self.hass,
                self._async_on_advertisement,
                bluetooth.BluetoothCallbackMatcher(address=self.address),
                bluetooth.BluetoothScanningMode.PASSIVE,
                replay=bluetooth.BluetoothCallbackReplay.DISABLED,
            )
        )
        if self._pending and self.interval_confirmed:
            self.entry.async_create_background_task(
                self.hass, self._async_import_pending_locked(), "atmo_history_pending"
            )

    @callback
    def async_stop(self) -> None:
        """Stop listening and cancel scheduled work."""
        for unsub in self._unsubs:
            unsub()
        self._unsubs.clear()
        self._cancel_schedule()

    async def async_confirm_interval(self, interval: int) -> None:
        """Mark ``interval`` as verified by the user."""
        self._confirmed_interval = interval
        await self._async_save()

    @property
    def _issue_id(self) -> str:
        return f"{ISSUE_INTERVAL_MISMATCH}_{self.entry.entry_id}"

    # Triggering --------------------------------------------------------

    @callback
    def _async_on_advertisement(
        self,
        service_info: bluetooth.BluetoothServiceInfoBleak,
        change: bluetooth.BluetoothChange,
    ) -> None:
        if (status := parse_advertisement(service_info.manufacturer_data)) is not None and (
            status != self.device_status
        ):
            self.device_status = status
            async_dispatcher_send(self.hass, SIGNAL_STATUS.format(self.entry.entry_id))
        now = monotonic()
        previous, self._last_seen = self._last_seen, now
        if self._blocked or self._lock.locked() or self._cancel_scheduled:
            return
        absent = float(self._opt(CONF_ABSENT_MINUTES, DEFAULT_ABSENT_MINUTES)) * 60
        retry = float(self._opt(CONF_RETRY_MINUTES, DEFAULT_RETRY_MINUTES)) * 60
        in_range = float(self._opt(CONF_IN_RANGE_MINUTES, DEFAULT_IN_RANGE_MINUTES)) * 60
        since_attempt = None if self._last_attempt is None else now - self._last_attempt
        if previous is None or now - previous >= absent:
            delay = float(self._opt(CONF_SYNC_DELAY, DEFAULT_SYNC_DELAY))
            _LOGGER.debug("%s reappeared, syncing in %s s", self.address, delay)
            self._schedule(delay, "reappeared")
        elif since_attempt is None:
            return
        elif self._failed:
            if since_attempt >= retry:
                _LOGGER.debug("%s still in range after a failed sync, retrying", self.address)
                self._schedule(0, "retry")
        elif in_range and since_attempt >= in_range:
            _LOGGER.debug("%s still in range, periodic sync", self.address)
            self._schedule(0, "periodic")

    def _schedule(self, delay: float, reason: str) -> None:
        @callback
        def _fire(_now: datetime) -> None:
            self._cancel_scheduled = None
            self.entry.async_create_background_task(
                self.hass, self.async_sync(reason), f"atmo_history_sync_{reason}"
            )

        self._cancel_scheduled = async_call_later(
            self.hass, delay, HassJob(_fire, cancel_on_shutdown=True)
        )

    def _cancel_schedule(self) -> None:
        if self._cancel_scheduled:
            self._cancel_scheduled()
            self._cancel_scheduled = None

    # Sync --------------------------------------------------------------

    async def async_sync(self, reason: str) -> None:
        """Run one sync session. Never raises; see ``status``."""
        if self._lock.locked():
            _LOGGER.debug("Sync already running, ignoring %s request", reason)
            return
        async with self._lock:
            self._cancel_schedule()
            await self._async_sync_locked(reason)

    async def _async_sync_locked(self, reason: str) -> None:
        _LOGGER.debug("Starting history sync for %s (%s)", self.address, reason)
        self._last_attempt = monotonic()
        self.status.last_attempt = dt_util.utcnow()
        self._session_imported = 0
        self._session_decoded = 0
        self._session_started = int(dt_util.utcnow().timestamp())
        self._session_resends = 0
        dry_run = bool(self._opt(CONF_DRY_RUN, False))

        ble_device = bluetooth.async_ble_device_from_address(
            self.hass, self.address, connectable=True
        )
        if ble_device is None:
            await self._async_finish(
                RESULT_UNAVAILABLE,
                failed=True,
                error="No connectable Bluetooth adapter or proxy can reach the device",
            )
            return

        outcome: str | None = None
        failed = False
        error: str | None = None
        result = None
        try:
            async with asyncio.timeout(SESSION_TIMEOUT):
                result = await async_download_history(
                    ble_device, self.name, self._async_handle_batch
                )
        except IntervalMismatch as err:
            self._blocked = True
            ir.async_create_issue(
                self.hass,
                DOMAIN,
                self._issue_id,
                is_fixable=False,
                severity=ir.IssueSeverity.WARNING,
                translation_key=ISSUE_INTERVAL_MISMATCH,
                translation_placeholders={
                    "name": self.name,
                    "measured": f"{err.measured:.1f}",
                    "configured": str(err.configured),
                },
            )
            _LOGGER.warning("%s; sync stopped until the interval is resolved", err)
            outcome, error = RESULT_INTERVAL_MISMATCH, str(err)
        except (
            ProtocolError,
            TransferAborted,
            BleakError,
            TimeoutError,
            HomeAssistantError,
        ) as err:
            error = str(err) or type(err).__name__
            _LOGGER.warning("History sync for %s failed: %s", self.address, error)
            outcome, failed = RESULT_ERROR, True
        except Exception as err:
            _LOGGER.exception("Unexpected error during history sync")
            outcome, failed, error = RESULT_ERROR, True, repr(err)

        # Batches acknowledged in this or earlier sessions are held in storage;
        # import them now, even if the transfer itself failed later on.
        if not dry_run and self._pending and self.interval_confirmed:
            try:
                await self._async_import_pending()
            except (influx.InfluxError, HomeAssistantError, ProtocolError) as err:
                _LOGGER.warning("Importing history for %s failed: %s", self.address, err)
                if outcome is None:
                    outcome, failed, error = RESULT_ERROR, True, f"Import failed: {err}"

        if outcome is None:
            if dry_run:
                outcome = RESULT_DRY_RUN
            elif self._pending:
                outcome = RESULT_AWAITING_INTERVAL
            elif self._session_imported == 0 and (result is None or result.batches_acked == 0):
                outcome = RESULT_NO_DATA
            else:
                outcome = RESULT_SUCCESS
        await self._async_finish(outcome, failed=failed, error=error)

    async def _async_finish(self, result: str, failed: bool, error: str | None) -> None:
        self._failed = failed
        self.status.last_result = result
        self.status.last_error = error
        self.status.last_records = (
            self._session_decoded if result == RESULT_DRY_RUN else self._session_imported
        )
        if result in (RESULT_SUCCESS, RESULT_NO_DATA, RESULT_AWAITING_INTERVAL):
            self.status.last_success = dt_util.utcnow()
        await self._async_save()
        async_dispatcher_send(self.hass, SIGNAL_UPDATED.format(self.entry.entry_id))

    async def _async_handle_batch(self, batch: HistoryBatch) -> bool:
        """Persist a complete batch. Returning True lets the device be ACKed."""
        header = batch.header
        interval = self.interval

        if self._opt(CONF_DRY_RUN, False):
            records = batch.records(interval)
            self._session_decoded += len(records)
            _LOGGER.info(
                "Dry run: batch at %s, %s records of %s bytes (not acknowledged, nothing stored)",
                header.first_timestamp,
                len(records),
                header.record_size,
            )
            for record in records:
                _LOGGER.info("Dry run record: %s", record)
            return False

        payload = batch.payload()
        first = header.first_timestamp
        skip = 0
        prev = self._last_batch
        if prev and is_resend(prev["first_timestamp"], prev["record_count"], first, interval):
            # The device sent the previous batch again (an acknowledgement was
            # not applied). Keep the earlier timestamps; only the tail is new.
            if (known := prev.get("payload")) is not None and not payload.startswith(
                bytes.fromhex(known)
            ):
                raise ProtocolError(
                    f"Device re-sent a batch starting near {prev['first_timestamp']} "
                    "with different data"
                )
            if batch.record_count < prev["record_count"]:
                raise ProtocolError(
                    f"Device re-sent a batch with {batch.record_count} records, "
                    f"fewer than the {prev['record_count']} already stored"
                )
            skip, first = prev["record_count"], prev["first_timestamp"]
            self._session_resends += 1
            _LOGGER.info(
                "Device re-sent %s already stored records; %s are new",
                skip,
                batch.record_count - skip,
            )
            if skip == batch.record_count and self._session_resends > 1:
                _LOGGER.warning("Device keeps re-sending the same records; stopping")
                return False
        else:
            first = self._check_interval(batch)

        if batch.record_count > skip:
            self._pending.append(
                {
                    "first_timestamp": first,
                    "record_count": batch.record_count,
                    "record_size": header.record_size,
                    "payload": payload.hex(),
                    "skip": skip,
                }
            )
            if not self.interval_confirmed:
                _LOGGER.info("Holding batch at %s until the record interval is confirmed", first)
        self._last_batch = {
            "first_timestamp": first,
            "record_count": batch.record_count,
            "payload": payload.hex(),
        }
        # Only the small store is written here so the ACK goes out quickly;
        # statistics and InfluxDB are written from the held batches afterwards.
        await self._async_save_sync_state()
        return True

    def _check_interval(self, batch: HistoryBatch) -> int:
        """Check the batch continues the previous one; return its start time.

        Confirms the record interval when it can, and raises IntervalMismatch
        when the batch would overlap earlier records or cannot be explained.
        """
        header = batch.header
        interval = self.interval
        if (
            not self.interval_confirmed
            and self._session_started is not None
            and batch_ends_at(
                header.first_timestamp, batch.record_count, interval, self._session_started
            )
        ):
            _LOGGER.info(
                "Record interval of %s s confirmed: the newest batch ends at the sync time",
                interval,
            )
            self._confirmed_interval = interval

        first = header.first_timestamp
        if not self._last_batch:
            return first
        prev_first = self._last_batch["first_timestamp"]
        prev_count = self._last_batch["record_count"]
        verdict, anchored = check_continuity(
            prev_first, prev_count, first, interval, self.interval_confirmed
        )
        if verdict == INTERVAL_MATCH:
            if not self.interval_confirmed:
                _LOGGER.info("Record interval of %s s confirmed", interval)
                self._confirmed_interval = interval
            return anchored
        if verdict == INTERVAL_GAP:
            _LOGGER.info(
                "Recording gap of %s s before %s",
                first - (prev_first + prev_count * interval),
                first,
            )
            return first
        if verdict == INTERVAL_MISMATCH:
            raise IntervalMismatch((first - prev_first) / prev_count, interval)
        return anchored

    async def _async_import_pending_locked(self) -> None:
        async with self._lock:
            try:
                await self._async_import_pending()
            except (influx.InfluxError, HomeAssistantError, ProtocolError) as err:
                _LOGGER.warning("Importing held batches failed: %s", err)
                return
            await self._async_save()
            async_dispatcher_send(self.hass, SIGNAL_UPDATED.format(self.entry.entry_id))

    async def _async_import_pending(self) -> None:
        while self._pending:
            entry = self._pending[0]
            batch = _batch_from_dict(entry)
            await self._async_import(batch.records(self.interval)[entry.get("skip", 0) :])
            self._pending.pop(0)
            await self._async_save()

    async def _async_import(self, records: list[HistoryRecord]) -> None:
        """Write to statistics and InfluxDB; only then keep the new state."""
        if not records:
            return
        aggregator = self._aggregator.copy()
        added, touched = await async_import_records(
            self.hass, self.address, self.name, aggregator, records, self.interval
        )
        if self._opt(CONF_INFLUX_ENABLED, False):
            await influx.async_write(
                async_get_clientsession(self.hass),
                self._opt(CONF_INFLUX_URL, ""),
                self._opt(CONF_INFLUX_TOKEN, ""),
                self._opt(CONF_INFLUX_ORG, ""),
                self._opt(CONF_INFLUX_BUCKET, ""),
                influx.to_line_protocol(self.address, records),
            )
        await self._async_write_entity_history(records, aggregator, touched)
        self._aggregator = aggregator
        self._session_imported += added
        _LOGGER.debug(
            "Imported %s new of %s records (%s to %s)",
            added,
            len(records),
            records[0].timestamp,
            records[-1].timestamp,
        )

    async def _async_write_entity_history(
        self,
        records: list[HistoryRecord],
        aggregator: HourlyAggregator,
        touched: set[int],
    ) -> None:
        """Give the history sensors the records as their recorded history.

        Each minute record becomes a state row with its original time, and the
        hourly statistics are attached to the entity as well, so history graphs
        show minute detail while the recorder keeps it and hourly data after.
        """
        rows: dict[str, list[tuple[float, str]]] = {}
        for metric, entity in self.history_entities.items():
            if entity.entity_id is None:
                continue
            points = [
                (record.timestamp, value)
                for record in records
                if (value := getattr(record, metric)) is not None
            ]
            if not points:
                continue
            # Record the newest reading as the live state first: that also
            # creates the entity's recorder metadata used by the backfill.
            entity.set_latest(points[-1][1], points[-1][0])
            rows[entity.entity_id] = [(float(ts), entity.render_state(v)) for ts, v in points]
        if not rows:
            return
        await async_backfill_states(self.hass, rows)

        for metric, entity in self.history_entities.items():
            if entity.entity_id not in rows:
                continue
            hourly = [
                StatisticData(
                    start=datetime.fromtimestamp(start, UTC),
                    mean=entity.convert(stats.mean),
                    min=entity.convert(stats.min),
                    max=entity.convert(stats.max),
                )
                for start in sorted(touched)
                if (stats := aggregator.stats(start).get(metric)) is not None
            ]
            if hourly:
                async_import_statistics(
                    self.hass,
                    StatisticMetaData(
                        mean_type=StatisticMeanType.ARITHMETIC,
                        has_sum=False,
                        name=None,
                        source=RECORDER_DOMAIN,
                        statistic_id=entity.entity_id,
                        unit_class=METRIC_META[metric][2],
                        unit_of_measurement=entity.unit_of_measurement,
                    ),
                    hourly,
                )
        await get_instance(self.hass).async_block_till_done()
