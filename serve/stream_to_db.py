"""Part 3, Serve: write the Kafka trip stream into PostgreSQL to answer Q3.

Q3: "How many trips are starting at each station this hour?"

This is a second consumer group on the divvy-trips topic, so it runs alongside
Part 2's console consumer without taking messages from it.

Every message first goes through the Part 4 quality checks
(quality/stream_checks.py). A message that fails is set aside in the
quarantine table with the checks it failed, and never reaches the counts.

For each batch of messages, one transaction:
  1. inserts the passing trips into trip_starts, skipping any trip_id already
     there (the duplicate_trip check);
  2. adds only the newly inserted trips to station_hourly_counts;
  3. puts the failing messages in quarantine;
  4. records this run's check results and row counts (quality_checks,
     stage_counts) and any alert (quality_alerts).
Kafka offsets are committed only after that transaction commits. If the
process dies between the two, Kafka hands the batch over again, step 1
skips every trip, step 3 skips every message already quarantined, and no
count is added twice.
"""
import argparse
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

from db import apply_schema, connect

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from quality import rules  # noqa: E402
from quality.stream_checks import check  # noqa: E402

TOPIC = "divvy-trips"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")  # "kafka:19092" inside Docker Compose
GROUP_ID = "divvy-serve-postgres"

UPSERT = """
WITH incoming AS (
    SELECT * FROM unnest(%s::BIGINT[], %s::INTEGER[], %s::TIMESTAMP[], %s::INTEGER[], %s::BIGINT[])
        AS t (trip_id, from_station_id, start_time, kafka_partition, kafka_offset)
),
inserted AS (
    INSERT INTO trip_starts (trip_id, from_station_id, start_time, kafka_partition, kafka_offset)
    SELECT * FROM incoming
    ON CONFLICT (trip_id) DO NOTHING
    RETURNING from_station_id, date_trunc('hour', start_time) AS hour_start
),
counted AS (
    INSERT INTO station_hourly_counts (station_id, hour_start, trips)
    SELECT from_station_id, hour_start, count(*) FROM inserted GROUP BY 1, 2
    ON CONFLICT (hour_start, station_id)
    DO UPDATE SET trips = station_hourly_counts.trips + EXCLUDED.trips, updated_at = now()
)
SELECT count(*) FROM inserted
"""

QUARANTINE = """
INSERT INTO quarantine (run_id, source, failed_checks, trip_id, raw, kafka_partition, kafka_offset)
VALUES (%s, 'stream', %s, %s, %s, %s, %s)
ON CONFLICT (kafka_partition, kafka_offset, md5(raw)) WHERE source = 'stream' DO NOTHING
"""

SAVE_CHECK = """
INSERT INTO quality_checks (run_id, pipeline, check_name, severity, description,
                            rows_checked, rows_failed, pct_failed, threshold_pct, status)
VALUES (%s, 'stream', %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, check_name) DO UPDATE SET
    rows_checked = EXCLUDED.rows_checked, rows_failed = EXCLUDED.rows_failed,
    pct_failed = EXCLUDED.pct_failed, status = EXCLUDED.status, checked_at = now()
"""

SAVE_STAGE = """
INSERT INTO stage_counts (run_id, pipeline, step, stage, rows_in, rows_out, rows_set_aside, note)
VALUES (%s, 'stream', %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, stage) DO UPDATE SET
    rows_in = EXCLUDED.rows_in, rows_out = EXCLUDED.rows_out,
    rows_set_aside = EXCLUDED.rows_set_aside, recorded_at = now()
"""

SAVE_ALERT = """
INSERT INTO quality_alerts (run_id, pipeline, check_name, rows_checked, rows_failed,
                            pct_failed, threshold_pct, message)
VALUES (%s, 'stream', %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, check_name) DO NOTHING
"""


def offset_meta(offset):
    # kafka-python 2.1+ added a leader_epoch field; older versions take two args.
    try:
        return OffsetAndMetadata(offset, None, -1)
    except TypeError:
        return OffsetAndMetadata(offset, None)


class RunStats:
    """This run's counts. Saved to PostgreSQL inside each batch's transaction."""

    def __init__(self, run_id, stations_known):
        self.run_id = run_id
        self.stations_known = stations_known
        self.received = self.passed = self.quarantined = self.new = self.duplicates = 0
        self.failed = Counter()
        self.alerted = set()

    def check_rows(self):
        """(check, rows checked, rows failed) for every stream check."""
        for c in rules.STREAM_CHECKS:
            if c == "duplicate_trip":       # only trips that passed reach the database
                yield c, self.passed, self.duplicates
            elif c == "unknown_station" and not self.stations_known:
                yield c, 0, 0
            else:
                yield c, self.received, self.failed[c]

    def save(self, conn):
        new_alerts = []
        for c, checked, failed in self.check_rows():
            pct, st = rules.status(c, failed, checked)
            if c == "unknown_station" and not self.stations_known:
                st = "SKIPPED"
            sev, desc, _ = rules.CHECKS[c]
            conn.execute(SAVE_CHECK, (self.run_id, c, sev, desc, checked, failed,
                                      round(pct, 4), rules.threshold_pct(c), st))
            if st == "ALERT" and c not in self.alerted:
                msg = (f"{c}: {failed} of {checked} messages failed ({pct:.2f}%), "
                       f"over the {rules.threshold_pct(c):g}% threshold")
                conn.execute(SAVE_ALERT, (self.run_id, c, checked, failed, round(pct, 4),
                                          rules.threshold_pct(c), msg))
                new_alerts.append(msg)
        stages = [
            (1, "messages read from Kafka", self.received, self.received, 0,
             f"topic {TOPIC}, consumer group {GROUP_ID}"),
            (2, "quality checks", self.received, self.passed, self.quarantined,
             "failing messages put in the quarantine table"),
            (3, "trip_starts (Q3)", self.passed, self.new, self.duplicates,
             "set aside = trip_id already stored (duplicate)"),
        ]
        for step, stage, rows_in, rows_out, aside, note in stages:
            conn.execute(SAVE_STAGE, (self.run_id, step, stage, rows_in, rows_out, aside, note))
        return new_alerts


def stop_on_sigterm(signum, frame):
    # `docker compose stop` sends SIGTERM. Turn it into the same clean shutdown
    # as Ctrl-C, so the consumer leaves its Kafka group straight away (and the
    # group can be reset) and the run's totals are printed.
    raise KeyboardInterrupt


def main():
    signal.signal(signal.SIGTERM, stop_on_sigterm)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--limit", type=int, default=0,
                        help="stop after this many messages (0 = no limit)")
    parser.add_argument("--idle-exit", type=float, default=0,
                        help="stop after this many seconds with no messages (0 = never)")
    args = parser.parse_args()

    conn = connect()
    apply_schema(conn)
    station_ids = {r[0] for r in conn.execute("SELECT station_id FROM stations")}
    if not station_ids:
        print("[serve] The stations table is empty, so the unknown_station check is skipped. "
              "Run serve/load_batch.py first.")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=500,
    )

    stats = RunStats(datetime.now(timezone.utc).strftime("stream-%Y%m%d-%H%M%S"), bool(station_ids))
    last_message = time.monotonic()
    print(f"[serve] Run {stats.run_id}: consuming '{TOPIC}' as group '{GROUP_ID}' into PostgreSQL...")

    try:
        while True:
            batches = consumer.poll(timeout_ms=1000)
            messages = [m for batch in batches.values() for m in batch]
            if args.limit:
                messages = messages[: args.limit - stats.received]

            if not messages:
                if args.idle_exit and time.monotonic() - last_message >= args.idle_exit:
                    print(f"[serve] No messages for {args.idle_exit:g} s, stopping.")
                    break
                continue
            last_message = time.monotonic()

            passed, rejected = [], []
            for m in messages:
                row, failed, raw_text, trip_id_text = check(m.value, station_ids)
                if failed:
                    rejected.append((failed, trip_id_text, raw_text, m.partition, m.offset))
                else:
                    passed.append((*row, m.partition, m.offset))

            stats.received += len(messages)
            stats.passed += len(passed)
            stats.quarantined += len(rejected)
            for failed, *_ in rejected:
                stats.failed.update(failed)

            with conn.transaction():
                new = conn.execute(UPSERT, list(map(list, zip(*passed)))).fetchone()[0] if passed else 0
                for failed, trip_id_text, raw_text, partition, offset in rejected:
                    conn.execute(QUARANTINE, (stats.run_id, failed, trip_id_text, raw_text,
                                              partition, offset))
                stats.new += new
                stats.duplicates += len(passed) - new
                alerts = stats.save(conn)

            # Offsets go to Kafka only after the rows are safely in PostgreSQL.
            # Commit exactly what was processed, so a --limit cut-off doesn't
            # skip the rest of the batch on the next run.
            last = {}
            for m in messages:
                last[TopicPartition(m.topic, m.partition)] = m.offset
            consumer.commit({tp: offset_meta(off + 1) for tp, off in last.items()})

            print(f"[serve] batch of {len(messages):>3}: {new} new, {len(passed) - new} already stored, "
                  f"{len(rejected)} quarantined  (total {stats.received})")
            for failed, trip_id_text, raw_text, partition, offset in rejected[:20]:
                print(f"[quality] REJECTED partition {partition} offset {offset}: "
                      f"failed {', '.join(failed)}  {raw_text[:120]}")
            for msg in alerts:
                stats.alerted.add(msg.split(":")[0])
                print(f"[quality] ALERT {msg}")

            if args.limit and stats.received >= args.limit:
                break
    except KeyboardInterrupt:
        print("\n[serve] Stopped (Ctrl-C or docker compose stop).")
    finally:
        consumer.close()
        conn.close()
        print(f"[serve] Run {stats.run_id}: read {stats.received} messages: "
              f"{stats.new} new trips stored, {stats.duplicates} duplicates skipped, "
              f"{stats.quarantined} quarantined"
              f"{' (' + ', '.join(f'{c} {n}' for c, n in stats.failed.items()) + ')' if stats.failed else ''}.")


if __name__ == "__main__":
    main()
