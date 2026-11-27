"""Part 4, Monitoring: one report of every check, row count and alert, from PostgreSQL.

  uv run --no-project python quality/report.py

Shows, for the batch pipeline and the latest stream run:
  - each quality check: rows checked, rows failed, status;
  - rows in and out of every stage, and whether they balance
    (rows in = rows out + rows set aside);
plus the Kafka topic's size, the stream reconciliation (stored trips vs the
Q3 counts), recent alerts and the quarantined rows.

Also saved to results/quality_report.txt for the report.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "serve"))
from db import connect  # noqa: E402

TOPIC = "divvy-trips"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")  # "kafka:19092" inside Docker Compose
OUT = ROOT / "results/quality_report.txt"
lines = []


def say(text=""):
    print(text)
    lines.append(text)


def table(headers, rows):
    rows = [["" if v is None else (f"{v:,}" if isinstance(v, int) else str(v)) for v in r] for r in rows]
    widths = [max(len(str(h)), *(len(r[i]) for r in rows)) if rows else len(str(h))
              for i, h in enumerate(headers)]
    say("  " + "  ".join(str(h).ljust(w) for h, w in zip(headers, widths)))
    say("  " + "  ".join("-" * w for w in widths))
    for r in rows:
        say("  " + "  ".join(v.rjust(w) if v.replace(",", "").replace(".", "").replace("%", "").isdigit()
                             else v.ljust(w) for v, w in zip(r, widths)))
    if not rows:
        say("  (none)")


def checks(conn, run_id):
    rows = conn.execute(
        "SELECT check_name, severity, rows_checked, rows_failed, pct_failed || '%%', "
        "       threshold_pct || '%%', status "
        "FROM quality_checks WHERE run_id = %s ORDER BY severity, check_name", (run_id,)).fetchall()
    table(["check", "severity", "rows checked", "rows failed", "% failed", "alert at", "status"], rows)


def stages(conn, run_id):
    rows = conn.execute(
        "SELECT step, stage, rows_in, rows_out, rows_set_aside, "
        "       CASE WHEN rows_in = rows_out + rows_set_aside THEN 'yes' ELSE 'NO' END, note "
        "FROM stage_counts WHERE run_id = %s ORDER BY step", (run_id,)).fetchall()
    table(["step", "stage", "rows in", "rows out", "set aside", "balanced", "note"], rows)


def kafka_topic_size():
    """Messages currently in the topic, from its partitions' offsets."""
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS, request_timeout_ms=5000)
        partitions = [TopicPartition(TOPIC, p) for p in consumer.partitions_for_topic(TOPIC) or []]
        if not partitions:
            consumer.close()
            return None
        end, begin = consumer.end_offsets(partitions), consumer.beginning_offsets(partitions)
        consumer.close()
        return sum(end[p] - begin[p] for p in partitions)
    except Exception as e:  # Kafka not running: the rest of the report still works
        return f"unavailable ({type(e).__name__})"


def main():
    with connect() as conn:
        say("=" * 78)
        say("PART 4  QUALITY AND MONITORING REPORT")
        say("=" * 78)

        batch = conn.execute("SELECT max(run_id) FROM quality_checks WHERE pipeline = 'batch'").fetchone()[0]
        say()
        say(f"BATCH PIPELINE  (run {batch or 'none yet: run Part 1, then serve/load_batch.py'})")
        if batch:
            say("\nQuality checks on the raw CSVs:")
            checks(conn, batch)
            say("\nRows in and out at every stage:")
            stages(conn, batch)

        stream = conn.execute("SELECT max(run_id) FROM quality_checks WHERE pipeline = 'stream'").fetchone()[0]
        say()
        say(f"STREAM PIPELINE  (latest run {stream or 'none yet: run serve/stream_to_db.py'})")
        size = kafka_topic_size()
        say(f"\n  Messages in Kafka topic '{TOPIC}': {size if not isinstance(size, int) else f'{size:,}'}")
        if stream:
            say("\nQuality checks on the Kafka messages (latest run):")
            checks(conn, stream)
            say("\nRows in and out at every stage (latest run):")
            stages(conn, stream)
            runs, received, stored, dupes, quarantined = conn.execute(
                "SELECT count(DISTINCT run_id), "
                "  sum(rows_in) FILTER (WHERE step = 1), sum(rows_out) FILTER (WHERE step = 3), "
                "  sum(rows_set_aside) FILTER (WHERE step = 3), sum(rows_set_aside) FILTER (WHERE step = 2) "
                "FROM stage_counts WHERE pipeline = 'stream'").fetchone()
            say(f"\n  All {runs} stream runs: {received:,} messages read, {stored:,} trips stored, "
                f"{dupes:,} duplicates skipped, {quarantined:,} quarantined")

        trips, counted, mismatches = conn.execute(
            "SELECT (SELECT count(*) FROM trip_starts), "
            "       (SELECT coalesce(sum(trips), 0) FROM station_hourly_counts), "
            "       (SELECT count(*) FROM ((SELECT station_id, hour_start, trips FROM station_hourly_counts "
            "                               EXCEPT SELECT * FROM station_hourly_recount) "
            "                              UNION ALL (SELECT * FROM station_hourly_recount "
            "                               EXCEPT SELECT station_id, hour_start, trips FROM station_hourly_counts)) d)"
        ).fetchone()
        say("\nStream reconciliation (Q3 counts vs the trips they were built from):")
        say(f"  trips stored in trip_starts:          {trips:,}")
        say(f"  sum of station_hourly_counts:         {counted:,}")
        say(f"  station-hours that disagree with a recount: {mismatches:,}  "
            f"-> {'OK' if trips == counted and mismatches == 0 else 'MISMATCH'}")

        say("\nALERTS (latest 10)")
        table(["raised at", "pipeline", "run", "message"], [
            (str(r[0])[:19], r[1], r[2], r[3]) for r in conn.execute(
                "SELECT raised_at, pipeline, run_id, message FROM quality_alerts "
                "ORDER BY raised_at DESC LIMIT 10")])

        say("\nQUARANTINE  (rows set aside, by source and failed check)")
        table(["source", "check", "rows"], conn.execute(
            "SELECT source, unnest(failed_checks) AS check_name, count(*) FROM quarantine "
            "GROUP BY 1, 2 ORDER BY 1, 2").fetchall())
        say("\nMost recent quarantined rows:")
        table(["id", "source", "failed checks", "trip_id", "where", "raw (first 70 characters)"], [
            (r[0], r[1], ",".join(r[2]), r[3], r[4], r[5][:70]) for r in conn.execute(
                "SELECT id, source, failed_checks, trip_id, "
                "  coalesce(source_file, 'partition ' || kafka_partition || ' offset ' || kafka_offset), raw "
                "FROM quarantine ORDER BY quarantined_at DESC, id DESC LIMIT 5")])

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text("\n".join(lines) + "\n")
    print(f"\n[report] saved to {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
