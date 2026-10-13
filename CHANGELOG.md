# Changelog

## 0.4.0

### Fixed

- Home Assistant's hourly statistics compile failed with
  `UNIQUE constraint failed: statistics.metadata_id, statistics.start_ts` (and, because one
  failed row rolls back the whole run, stopped updating statistics for every entity, including
  the Energy dashboard). The history sensors have a state class, so the recorder compiles their
  statistics itself, and the integration also imported hourly statistics into the same
  statistic ids (`sensor.*_history`). It no longer does: hourly history is written only to the
  integration's own external statistics (`atmo_history:<mac>_<metric>`).

### Changed

- Hourly statistics are written only for hours that have ended. Records from the hour in
  progress are kept and written on the first sync after it ends, even if that sync brings no
  new records.
- The history sensors' own long-term statistics now come only from the recorder's compile of
  their states. Chart the `atmo_history:` statistics for accurate hourly history.

### Upgrading

- Nothing is deleted. Hourly rows imported into the `sensor.*_history` statistics by earlier
  versions stay as they are.
- The `atmo_history:` statistics already held all imported history, so there is nothing to
  backfill.
