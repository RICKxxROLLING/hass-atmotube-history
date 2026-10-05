# Atmotube History (`atmo_history`)

A Home Assistant custom integration that downloads the measurement history stored on an
**original Atmotube PRO** (not the PRO 2) over Bluetooth whenever it comes into range, and
imports it with the original timestamps:

- as **history sensors** whose recorded history holds every minute record,
- as **long-term statistics** (hourly mean/min/max per metric), merged into hours that already
  have data rather than overwriting them, and
- optionally as **raw per-record points in InfluxDB v2**.

While the device is in range it syncs every few minutes, so the history sensors stay close to
live. Battery level and charging state come from the device's broadcasts. That makes
[ha-atmo](https://github.com/natekspencer/ha-atmo) (`atmo`) optional: keep it if you want readings
within seconds (e.g. for automations), or remove it. If both are installed, their entities share
one device, because both identify it by its Bluetooth address.

## Requirements

- Home Assistant 2026.9 or newer.
- A connectable Bluetooth path to the device. A local adapter works, and so does an
  **ESPHome Bluetooth proxy with `active: true`**:

  ```yaml
  bluetooth_proxy:
    active: true
  ```

  A passive proxy can see advertisements but cannot connect, so history cannot be downloaded
  through it. ESPHome proxies have a small number of connection slots (3 by default), shared
  with every other integration that connects through them.

## Things that will stop you getting data

**The phone app must stop syncing.** The device deletes history from its "unsynced" queue as
soon as any client acknowledges it. Whichever client acknowledges first (the Atmotube app, or
this integration) gets that data, and the other never sees it. Turn off background sync in the
Atmotube app, or keep the phone away from the device, or you will get gaps.

**ha-atmo's "Enable polling" competes for the connection.** If that option is on, ha-atmo
connects to the device to read extra values. The device takes one connection at a time, so a
history sync can fail to connect, or be cut off, while ha-atmo is connected. Failed syncs are
retried (see below) and never lose data, but turning polling off avoids the contention.

## Installation

1. HACS → three dots → *Custom repositories* → add this repository as an **Integration**.
2. Install *Atmotube History* and restart Home Assistant.
3. The device is discovered automatically (local name `ATMOTUBE` and service UUID
   `DB450001-8E9A-4818-ADD7-6ED94A328AB4`). You can also add it under
   *Settings → Devices & services → Add integration → Atmotube History* and type the MAC address.

## How syncing works

- The integration listens for the device's advertisements. When the device **reappears after
  being out of range for at least 10 minutes** (configurable), it waits **30 s** and then syncs.
  The first sighting after Home Assistant starts also counts as a reappearance.
- While the device stays in range it **syncs every 5 minutes** (configurable, 0 turns it off), so
  the history sensors stay close to live while the car is parked.
- If a sync fails, it is **retried every 15 minutes** (configurable) while the device stays in
  range.
- `atmo_history.sync_now` runs a sync on demand. It takes an optional `config_entry_id`.

The transfer uses the Nordic UART service (`6E400001-…`). The integration sends `HST` plus the
current time. The device then sends batches, each an `HT` header followed by `HD` data packets.
Each **complete** batch is first saved as raw bytes in Home Assistant's storage, and only then
acknowledged with `HOK`. The device waits only about **5 seconds** for that acknowledgement
before sending the batch again, which is too short to wait for the recorder. So statistics and
InfluxDB are written from the saved batch straight after. If either fails, the batch stays saved
and the import is retried on the next sync. Nothing is deleted until it has been imported.

A disconnect, a timeout, a missing or out-of-step packet, or a failure to save means the batch
is **not** acknowledged, so the device keeps it and sends it again next time. If the device sends
records that are already stored (for example because an acknowledgement arrived late), they are
recognised by their bytes and start time, and only the new records are imported.

## Protocol details that are not officially documented

Atmotube's [Bluetooth API article](https://support.atmotube.com/en/articles/10364981-bluetooth-api)
leaves some details out. This is how the integration handles each one:

| Detail | What the integration does | Source |
|---|---|---|
| Timestamp byte order (`HST`/`HOK`/`HT`) | Big-endian. Record fields are little-endian. | [atmotuber](https://github.com/AtzeniMichele/atmotuber) `utils.dart`, confirmed by a real capture |
| Interval between records | 60 s by default, **checked automatically** (see below) | atmotuber, and a real capture (37 records ending exactly at the sync time) |
| `HT` count and `HD` number bytes | A **record** count and a **running record total**, not packet counts. Over a proxy each `HD` carries up to 15 records (e.g. `HT` 37, then `HD` 15, 30, 37). | Real capture. atmotuber's `diff` logic agrees. |
| Record size | 16 bytes: the 14 documented bytes, then 2 unknown bytes (always 0 so far) | atmotuber and a real capture |
| Acknowledgement window | About 5 s after the last `HD`; after that the device sends the batch again | Real capture |
| "No reading" markers | `0x80` temperature/humidity, `0xFFFF` VOC/PM, `0xFFFFFFFF` pressure: skipped | Real capture (the pressure marker is assumed by analogy); Atmotube's Android library for PM |

**Interval check.** The interval is never assumed. The device works out each batch's start by
counting back from the time Home Assistant sends when the sync starts, so a start time is only
accurate to about one interval. The interval is confirmed in either of two ways:

- the newest batch, spaced at the configured interval, ends at the time of the sync, or
- a batch of at least 10 records is followed by one that starts within one interval of where it
  ended.

Until one of those has matched once, downloaded batches are **kept as raw bytes in Home
Assistant's storage and acknowledged**, but not imported. Once a match is seen, everything held
is imported.

A batch that starts within one interval of where the previous one ended continues it, and is
shifted onto the same minute grid so the timestamps stay evenly spaced. A batch that starts
later than that is a recording gap (the device was off). If a batch would **overlap** earlier
records, or (before confirmation) cannot be explained, the sync **stops without acknowledging**
and a repair issue explains what to do. Either set *Seconds between history records* to the
right value, or tick *I have verified the record interval* if the difference was a recording
gap.

A first-record time before 2016 or in the future aborts the batch without acknowledging it. If
reading the bytes in the other order would give a sensible time, the error says so.

## Verifying against your own device

1. Turn on debug logging:

   ```yaml
   logger:
     logs:
       custom_components.atmo_history: debug
   ```

   The raw `HT`/`HD` packets (`RX HT: 4854...`), the commands sent (`TX: 485354...`) and the
   decoded headers are logged.
2. For the first sync, enable **Dry run** in the options. It downloads the first batch, logs
   every decoded record at info level, stores nothing and **never acknowledges**, so the data
   stays on the device. Compare the values and timestamps with the Atmotube app, then turn dry
   run off.

## Options

| Option | Default | |
|---|---|---|
| Absence before a new sync | 10 min | |
| Delay after the device reappears | 30 s | |
| Retry interval after a failed sync | 15 min | |
| Sync every N minutes while in range | 5 min | 0 = only when the device reappears |
| Seconds between history records | 60 | See the interval check above |
| I have verified the record interval | off | Only acts at the moment you save the form |
| Dry run | off | |
| InfluxDB v2 URL / token / org / bucket | — | Tested when saved. A failed write is retried on the next sync. |

## What you get

**History sensors (minute by minute).** `PM1 history`, `PM2.5 history`, `PM10 history`,
`VOC history`, `Pressure history`, `Temperature history` and `Humidity history`. Each one's state
is the newest downloaded reading. Every downloaded record is written into the entity's recorded
history **with its original timestamp**, so the History panel and history graph cards show the
full minute-by-minute data, including the time the car was away.

- Minute rows are kept as long as the recorder keeps any entity history (`purge_keep_days`, 10
  days by default). Hourly mean/min/max is also attached to these entities as long-term
  statistics, so graphs of older periods still show hourly data.
- Home Assistant has no public API for recording states in the past. The integration inserts
  these rows through a task on the recorder's own queue, the same way the recorder imports
  statistics. A future Home Assistant update could change the database layout and break this
  until the integration is updated. If that happens, the import reports an error and the
  downloaded batches stay saved and are retried.
- If you exclude these entities from the recorder, their minute history is skipped (with a
  warning in the log).
- A display unit chosen in the entity's settings (e.g. hPa for pressure) is applied to the
  backfilled rows too.

**Statistics** (use them in *Statistics graph* cards, or under *Developer tools → Statistics*):

| Statistic ID | Unit |
|---|---|
| `atmo_history:<mac>_temperature` | °C |
| `atmo_history:<mac>_humidity` | % |
| `atmo_history:<mac>_voc` | ppb |
| `atmo_history:<mac>_pressure` | Pa |
| `atmo_history:<mac>_pm1`, `_pm25`, `_pm10` | µg/m³ |

The units match ha-atmo's live sensors. External statistics have a unit but no device class,
because Home Assistant does not store one for them.

Merging is exact for the last 30 days. When newer data lands in an hour older than that, the
existing row is weighted as if it covered the rest of the hour.

**InfluxDB** gets a measurement `atmotube_history`, tagged `device=<MAC>`, with float fields
`temperature humidity voc pressure pm1 pm25 pm10` at second precision. PM fields are left out
when the sensor was off.

**Battery and charging:** *Battery* (%) and *Charging* are read from the device's Bluetooth
broadcasts, so they update whenever it is in range, without a connection. They keep their last
value while the car is away.

**Diagnostic sensors:**

- *Last history sync*: the time of the last sync that succeeded.
- *History records imported*: the number of new records from the last sync. In dry run it shows
  the number decoded instead. The `held_batches` attribute counts batches waiting for the
  interval check.
- *Last history sync result*: one of `success`, `no_data`, `dry_run`, `awaiting_interval_check`,
  `interval_mismatch`, `device_unavailable` or `error`. The `error` attribute has the details.

## Development

```bash
pip install -r requirements_test.txt   # Python 3.14
pytest
```

`protocol.py` and `aggregate.py` don't import Home Assistant or bleak. They are tested with
hand-built packet fixtures: multi-packet and multi-batch transfers, empty history, truncated
transfers, disconnects, inconsistent packets and byte-order detection. The config flow, the
trigger timing and end-to-end imports into a real in-memory recorder are tested with
`pytest-homeassistant-custom-component`.
