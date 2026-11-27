"""Part 4, Quality: the checks run on every Kafka message before it reaches PostgreSQL.

Same rules as the batch checks (quality/rules.py), applied to the three fields
the producer sends: trip_id, start_time and from_station_id. The fifth stream
check, duplicate_trip, is done by the database itself (the primary key on
trip_starts.trip_id) and counted in serve/stream_to_db.py.
"""
import json
from datetime import datetime

from quality import rules


def check(value: bytes, station_ids: set[int]):
    """Check one message.

    Returns (row, failed_checks, raw_text, trip_id_text):
      row            (trip_id, from_station_id, start_time) if every check passed, else None
      failed_checks  names of the checks it failed, empty if none
    """
    try:
        raw_text = value.decode("utf-8")
    except UnicodeDecodeError:
        return None, ["schema"], value.decode("utf-8", errors="replace"), None

    try:
        event = json.loads(raw_text)
    except json.JSONDecodeError:
        event = None
    if not isinstance(event, dict):
        return None, ["schema"], raw_text, None

    failed = []
    trip_id_text = None if event.get("trip_id") is None else str(event.get("trip_id"))

    missing = [f for f in rules.REQUIRED_STREAM_FIELDS if event.get(f) in (None, "")]
    if missing:
        failed.append("missing_required")

    trip_id = station = start = None
    try:
        if "trip_id" not in missing:
            trip_id = int(str(event["trip_id"]))
        if "from_station_id" not in missing:
            station = int(str(event["from_station_id"]))
        if "start_time" not in missing:
            start = datetime.strptime(str(event["start_time"]), rules.TIMESTAMP_FORMAT)
    except ValueError:
        failed.append("schema")

    if start is not None and not (rules.DATA_START <= start < rules.DATA_END):
        failed.append("impossible_range")
    if trip_id is not None and trip_id <= 0:
        failed.append("impossible_range")
    # Skipped when the stations table is empty (load_batch.py hasn't run yet).
    if station is not None and station_ids and station not in station_ids:
        failed.append("unknown_station")

    failed = list(dict.fromkeys(failed))  # each check once, in order
    row = (trip_id, station, start) if not failed else None
    return row, failed, raw_text, trip_id_text
