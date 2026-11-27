"""Part 2: replay the Divvy trips into Kafka in event time order, sped up.

The Divvy CSVs run newest trip first, and Q1 and Q2 are separate files, so
reading them top to bottom would send the stream backwards in time. Instead
the producer reads every trip, sorts by start_time, and sends oldest first.

To fit 1.5 million trips in the container's 512 MB, the three fields sent are
kept as numbers in compact arrays (about 30 MB) rather than as Python objects.

Rows that can't be read (a blank or non-numeric trip_id, station or start
time, for example a bad row added by hand for the demo) are not dropped here:
they are sent first, exactly as they appear in the CSV, so the stream quality
checks in serve/stream_to_db.py catch them and put them in quarantine.

Pacing: --delay waits a fixed time between messages. --speedup instead waits
in proportion to the gap between trips' start times (--speedup 3600 plays one
hour of trips per second). --limit 0 sends every trip.
"""
import argparse
import csv
import os
import json
import time
from array import array
from datetime import datetime, timedelta
from pathlib import Path

from kafka import KafkaProducer

TOPIC = "divvy-trips"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")  # "kafka:19092" inside Docker Compose
TIMESTAMP_FORMAT = "%m/%d/%Y %H:%M:%S"  # Divvy's format, e.g. "3/31/2017 23:59:07"
EPOCH = datetime(1970, 1, 1)
MAX_SLEEP_SECONDS = 2.0  # with --speedup, don't stall on a quiet night

DATA_FILES = [
    Path("data/raw/Divvy_Trips_2017_Q1.csv"),
    Path("data/raw/Divvy_Trips_2017_Q2.csv"),
]


def divvy_time(seconds):
    """Seconds since 1970 back to Divvy's text format, e.g. 3/31/2017 23:59:07."""
    t = EPOCH + timedelta(seconds=seconds)
    return f"{t.month}/{t.day}/{t.year} {t:%H:%M:%S}"


def load_trips():
    """Every trip's start time, trip ID and station, plus the rows that didn't parse."""
    starts, trip_ids, stations = array("q"), array("q"), array("i")
    unreadable = []
    for path in DATA_FILES:
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            for row in csv.DictReader(file):
                try:
                    start = datetime.strptime(row["start_time"], TIMESTAMP_FORMAT)
                    trip_id, station = int(row["trip_id"]), int(row["from_station_id"])
                except (ValueError, TypeError, KeyError):
                    unreadable.append({k: row.get(k) for k in ("trip_id", "start_time", "from_station_id")})
                    continue
                starts.append(int((start - EPOCH).total_seconds()))
                trip_ids.append(trip_id)
                stations.append(station)
    return starts, trip_ids, stations, unreadable


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100, help="messages to send (0 = all)")
    parser.add_argument("--delay", type=float, default=0.05, help="seconds between messages")
    parser.add_argument("--speedup", type=float, default=0,
                        help="pace by event time instead: N seconds of trips per second (overrides --delay)")
    args = parser.parse_args()

    t0 = time.perf_counter()
    starts, trip_ids, stations, unreadable = load_trips()
    # Sort by start time; ties keep file order. Only this index list is big.
    order = sorted(range(len(starts)), key=starts.__getitem__)
    print(f"[producer] Read {len(starts):,} trips and {len(unreadable)} unreadable rows, "
          f"sorted by start_time in {time.perf_counter() - t0:.1f} s.")
    if starts:
        print(f"[producer] Replaying from {divvy_time(starts[order[0]])} "
              f"to {divvy_time(starts[order[-1]])} (event time order).")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    def events():
        for raw in unreadable:          # first, so the quality checks see them in any demo
            yield None, raw
        for i in order:
            yield starts[i], {"trip_id": str(trip_ids[i]), "start_time": divvy_time(starts[i]),
                              "from_station_id": stations[i]}

    count, previous = 0, None
    for event_time, event in events():
        if args.speedup > 0 and event_time is not None and previous is not None:
            time.sleep(min((event_time - previous) / args.speedup, MAX_SLEEP_SECONDS))
        elif args.speedup <= 0 and args.delay > 0:
            time.sleep(args.delay)
        if event_time is not None:
            previous = event_time

        producer.send(TOPIC, key=event.get("from_station_id"), value=event)
        count += 1
        if count <= 10 or count % 1000 == 0 or event_time is None:
            print(f"[producer] Sent {count}: {event}{'  (unreadable row, sent as-is)' if event_time is None else ''}")
        if args.limit and count >= args.limit:
            break

    producer.flush()
    producer.close()
    print(f"[producer] Finished sending {count:,} events.")


if __name__ == "__main__":
    main()
