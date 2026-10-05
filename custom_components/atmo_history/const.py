"""Constants for Atmotube history."""

from __future__ import annotations

DOMAIN = "atmo_history"

ATMOTUBE_PRO_SERVICE_UUID = "db450001-8e9a-4818-add7-6ed94a328ab4"
ATMOTUBE_LOCAL_NAME = "ATMOTUBE"

CONF_ABSENT_MINUTES = "absent_minutes"
CONF_SYNC_DELAY = "sync_delay"
CONF_RETRY_MINUTES = "retry_minutes"
CONF_IN_RANGE_MINUTES = "in_range_minutes"
CONF_RECORD_INTERVAL = "record_interval"
CONF_CONFIRM_INTERVAL = "confirm_interval"
CONF_DRY_RUN = "dry_run"
CONF_INFLUX_ENABLED = "influx_enabled"
CONF_INFLUX_URL = "influx_url"
CONF_INFLUX_TOKEN = "influx_token"
CONF_INFLUX_ORG = "influx_org"
CONF_INFLUX_BUCKET = "influx_bucket"

DEFAULT_ABSENT_MINUTES = 10
DEFAULT_SYNC_DELAY = 30
DEFAULT_RETRY_MINUTES = 15
DEFAULT_IN_RANGE_MINUTES = 5
DEFAULT_RECORD_INTERVAL = 60

SESSION_TIMEOUT = 900
# Hours newer than this are tracked exactly for merging.
TRACKED_HOURS_DAYS = 30

SERVICE_SYNC_NOW = "sync_now"
ATTR_CONFIG_ENTRY_ID = "config_entry_id"

RESULT_SUCCESS = "success"
RESULT_NO_DATA = "no_data"
RESULT_DRY_RUN = "dry_run"
RESULT_AWAITING_INTERVAL = "awaiting_interval_check"
RESULT_INTERVAL_MISMATCH = "interval_mismatch"
RESULT_UNAVAILABLE = "device_unavailable"
RESULT_ERROR = "error"
RESULTS = [
    RESULT_SUCCESS,
    RESULT_NO_DATA,
    RESULT_DRY_RUN,
    RESULT_AWAITING_INTERVAL,
    RESULT_INTERVAL_MISMATCH,
    RESULT_UNAVAILABLE,
    RESULT_ERROR,
]

ISSUE_INTERVAL_MISMATCH = "interval_mismatch"

SIGNAL_UPDATED = f"{DOMAIN}_updated_{{}}"
