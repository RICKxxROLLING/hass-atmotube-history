"""Backfill minute-by-minute entity history into the recorder.

Home Assistant has no public API for recording states in the past, so this
queues a task on the recorder's own thread (the same mechanism it uses to
import statistics) that inserts rows into the ``states`` table with the
records' original timestamps. The entity must already have one state recorded,
which provides its ``states_meta`` and ``state_attributes`` rows to reuse.
The rows are purged with the rest of the entity history (``purge_keep_days``).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from homeassistant.components.recorder import Recorder, get_instance
from homeassistant.components.recorder.db_schema import States, StatesMeta
from homeassistant.components.recorder.tasks import RecorderTask
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.recorder import session_scope
from homeassistant.util.ulid import ulid_at_time, ulid_to_bytes
from sqlalchemy import insert, select

_LOGGER = logging.getLogger(__name__)

# Rows within this many seconds of an existing row are treated as present.
DUPLICATE_TOLERANCE = 0.5


@dataclass(slots=True)
class BackfillResult:
    """Rows inserted per entity, and entities that could not be backfilled."""

    inserted: dict[str, int]
    skipped: list[str]


def _backfill(
    instance: Recorder, rows: Mapping[str, Sequence[tuple[float, str]]]
) -> BackfillResult:
    result = BackfillResult(inserted={}, skipped=[])
    with session_scope(session=instance.get_session()) as session:
        for entity_id, entity_rows in rows.items():
            if not entity_rows:
                continue
            metadata_id = session.execute(
                select(StatesMeta.metadata_id).where(StatesMeta.entity_id == entity_id)
            ).scalar_one_or_none()
            if metadata_id is None:
                # Not recorded yet, or excluded from the recorder.
                result.skipped.append(entity_id)
                continue
            attributes_id = session.execute(
                select(States.attributes_id)
                .where(States.metadata_id == metadata_id)
                .order_by(States.last_updated_ts.desc())
                .limit(1)
            ).scalar_one_or_none()

            low = min(ts for ts, _ in entity_rows) - 1
            high = max(ts for ts, _ in entity_rows) + 1
            existing = sorted(
                ts
                for ts in session.execute(
                    select(States.last_updated_ts).where(
                        States.metadata_id == metadata_id,
                        States.last_updated_ts >= low,
                        States.last_updated_ts <= high,
                    )
                ).scalars()
                if ts is not None
            )

            def present(ts: float, existing: list[float] = existing) -> bool:
                return any(abs(ts - other) <= DUPLICATE_TOLERANCE for other in existing)

            values = [
                {
                    "state": state,
                    "metadata_id": metadata_id,
                    "attributes_id": attributes_id,
                    "last_updated_ts": ts,
                    "last_changed_ts": None,  # same as last_updated
                    "last_reported_ts": None,
                    "origin_idx": 0,
                    "context_id_bin": ulid_to_bytes(ulid_at_time(ts)),
                }
                for ts, state in entity_rows
                if not present(ts)
            ]
            if values:
                session.execute(insert(States), values)
            result.inserted[entity_id] = len(values)
    return result


class BackfillTask(RecorderTask):
    """Recorder task that inserts historical states."""

    commit_before = True

    def __init__(
        self,
        rows: Mapping[str, Sequence[tuple[float, str]]],
        future: asyncio.Future[BackfillResult],
    ) -> None:
        """Initialize."""
        self.rows = rows
        self.future = future

    def run(self, instance: Recorder) -> None:
        """Insert the rows and resolve the future on the event loop."""
        loop = instance.hass.loop
        try:
            result = _backfill(instance, self.rows)
        except Exception as err:
            loop.call_soon_threadsafe(_set_exception, self.future, err)
        else:
            loop.call_soon_threadsafe(_set_result, self.future, result)


def _set_result(future: asyncio.Future[BackfillResult], result: BackfillResult) -> None:
    if not future.done():
        future.set_result(result)


def _set_exception(future: asyncio.Future[BackfillResult], err: Exception) -> None:
    if not future.done():
        future.set_exception(err)


async def async_backfill_states(
    hass: HomeAssistant, rows: Mapping[str, Sequence[tuple[float, str]]]
) -> BackfillResult:
    """Insert ``(timestamp, state)`` rows per entity and wait for the commit."""
    if not any(rows.values()):
        return BackfillResult(inserted={}, skipped=[])
    instance = get_instance(hass)
    future: asyncio.Future[BackfillResult] = hass.loop.create_future()
    instance.queue_task(BackfillTask(rows, future))
    try:
        result = await future
    except Exception as err:
        raise HomeAssistantError(f"Writing entity history failed: {err}") from err
    await instance.async_block_till_done()
    if result.skipped:
        _LOGGER.warning(
            "No recorded state yet for %s (excluded from the recorder?); "
            "minute history not written for them",
            ", ".join(result.skipped),
        )
    return result
