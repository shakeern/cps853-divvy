"""Part 4, Quality: the rules every trip is checked against.

One place for every limit, shared by the batch checks (Spark, before Part 1
converts the CSVs) and the stream checks (in the Kafka -> PostgreSQL sink),
so both halves of the pipeline judge a trip the same way.
"""
import os
from datetime import datetime

# The dataset is 2017 Q1 and Q2, in Chicago local time.
DATA_START = datetime(2017, 1, 1)
DATA_END = datetime(2017, 7, 1)          # exclusive
TIMESTAMP_FORMAT = "%m/%d/%Y %H:%M:%S"   # Divvy's format, e.g. "3/31/2017 23:59:07"

MAX_TRIP_SECONDS = 24 * 3600             # Divvy treats a bike out over a day as lost
BIRTHYEAR_MIN, BIRTHYEAR_MAX = 1920, 2005
DURATION_TOLERANCE_SECONDS = 120         # tripduration vs end_time - start_time
USERTYPES = ("Subscriber", "Customer", "Dependent")
GENDERS = ("Male", "Female")             # blank is allowed: day-pass riders aren't asked

REQUIRED_BATCH_FIELDS = ("trip_id", "start_time", "end_time", "tripduration",
                         "from_station_id", "to_station_id", "usertype")
REQUIRED_STREAM_FIELDS = ("trip_id", "start_time", "from_station_id")

# name: (severity, what it catches, alert threshold as % of rows checked)
# "reject": the row is wrong; it is set aside in the quarantine table.
# "warn":   the row is suspicious but still a real trip; it is counted only.
CHECKS = {
    "schema":               ("reject", "a value doesn't parse as its type, or the row has the wrong number of columns", 0.1),
    "missing_required":     ("reject", "a required field is blank", 0.1),
    "impossible_range":     ("reject", "duration <= 0 or over 24 h, end before start, or start outside Jan-Jun 2017", 0.1),
    "invalid_category":     ("reject", "usertype or gender is not one of the known values", 0.1),
    "duplicate_trip":       ("reject", "the same trip_id appears again (the first copy is kept)", 0.1),
    "unknown_station":      ("reject", "a station ID that isn't in the stations table", 0.1),
    "suspicious_birthyear": ("warn",   f"birth year outside {BIRTHYEAR_MIN}-{BIRTHYEAR_MAX}", 1.0),
    "duration_mismatch":    ("warn",   f"tripduration differs from end - start by over {DURATION_TOLERANCE_SECONDS} s", 1.0),
}
BATCH_CHECKS = list(CHECKS)
STREAM_CHECKS = ["schema", "missing_required", "impossible_range", "unknown_station", "duplicate_trip"]

# Don't alert on a handful of rows: 1 bad row out of 3 is 33 %, but says little.
ALERT_MIN_ROWS = int(os.environ.get("QUALITY_ALERT_MIN_ROWS", "100"))


def threshold_pct(check: str) -> float:
    """Alert threshold for a check. QUALITY_ALERT_PCT overrides every check (for demos)."""
    override = os.environ.get("QUALITY_ALERT_PCT")
    return float(override) if override else CHECKS[check][2]


def status(check: str, rows_failed: int, rows_checked: int) -> tuple[float, str]:
    """(percent failed, status). Status is PASS, FAIL or WARN under the threshold, ALERT over it."""
    pct = 100.0 * rows_failed / rows_checked if rows_checked else 0.0
    if rows_failed == 0:
        return pct, "PASS"
    if rows_checked >= ALERT_MIN_ROWS and pct > threshold_pct(check):
        return pct, "ALERT"
    return pct, "FAIL" if CHECKS[check][0] == "reject" else "WARN"
