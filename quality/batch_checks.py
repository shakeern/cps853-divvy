"""Part 4, Quality: check every raw trip before Part 1 converts it to Parquet.

The first stage of run_part1.sh. Reads the raw CSVs as text, runs every check
in quality/rules.py on every row, and reports how many rows failed each one.

Writes, for serve/load_batch.py to put in PostgreSQL:
  results/quality_checks.csv        one line per check: rows checked, failed, status
  results/quality_stage_counts.csv  rows in and out of this stage
  output/quarantine_batch.csv       every rejected row, its failed checks and the raw values

If any check fails on more rows than its alert threshold, the job prints an
ALERT and exits with status 3, which stops run_part1.sh before the bad data is
converted. Set QUALITY_FAIL_ON_ALERT=0 to report the alert and carry on.
"""
import csv
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import Window, functions as F, types as T

from common import OUTPUT, RESULTS, TRIPS_CSV, TRIPS_RAW_SCHEMA, TS_FMT, read_stations_csv, spark_session
from quality import rules

RAW_COLUMNS = [f.name for f in TRIPS_RAW_SCHEMA.fields]
QUARANTINE_CSV = f"{OUTPUT}/quarantine_batch.csv"


def blank(c):
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def unparsable(c, typed):
    """There is a value, but it didn't convert to its type."""
    return ~blank(c) & typed.isNull()


def chicago(dt):
    """A Python datetime as a Spark timestamp in Chicago time (the session time
    zone), like the trip times. F.lit(dt) would use the machine's own zone, which
    is UTC in the container, and move the cut-off by five or six hours."""
    return F.to_timestamp(F.lit(dt.strftime("%Y-%m-%d %H:%M:%S")))


def main():
    run_id = datetime.now(timezone.utc).strftime("batch-%Y%m%d-%H%M%S")  # UTC, like the stream runs
    spark = spark_session("divvy-quality")

    # Read everything as text, plus a column that catches rows Spark can't
    # split into the 12 expected fields.
    schema = T.StructType(TRIPS_RAW_SCHEMA.fields + [T.StructField("_corrupt_record", T.StringType())])
    raw = (spark.read
           .option("header", True)
           .option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord", "_corrupt_record")
           .schema(schema)
           .csv(TRIPS_CSV)
           .withColumn("source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
           .withColumn("row_seq", F.monotonically_increasing_id()))

    trip_id = F.col("trip_id").cast("long")
    start = F.to_timestamp("start_time", TS_FMT)
    end = F.to_timestamp("end_time", TS_FMT)
    duration = F.col("tripduration").cast("int")
    from_station = F.col("from_station_id").cast("int")
    to_station = F.col("to_station_id").cast("int")
    birthyear = F.col("birthyear").cast("int")
    station_ids = [r.station_id for r in read_stations_csv(spark).select("station_id").collect()]

    first_copy = Window.partitionBy(trip_id).orderBy("source_file", "row_seq")

    checks = {
        "schema": F.col("_corrupt_record").isNotNull()
            | unparsable("trip_id", trip_id) | unparsable("start_time", start)
            | unparsable("end_time", end) | unparsable("bikeid", F.col("bikeid").cast("int"))
            | unparsable("tripduration", duration) | unparsable("from_station_id", from_station)
            | unparsable("to_station_id", to_station) | unparsable("birthyear", birthyear),
        "missing_required": F.greatest(*[blank(c) for c in rules.REQUIRED_BATCH_FIELDS]),
        "impossible_range": (duration <= 0) | (duration > rules.MAX_TRIP_SECONDS) | (end < start)
            | (start < chicago(rules.DATA_START)) | (start >= chicago(rules.DATA_END)),
        "invalid_category": (~blank("usertype") & ~F.col("usertype").isin(*rules.USERTYPES))
            | (~blank("gender") & ~F.col("gender").isin(*rules.GENDERS)),
        "duplicate_trip": trip_id.isNotNull() & (F.row_number().over(first_copy) > 1),
        "unknown_station": (from_station.isNotNull() & ~from_station.isin(*station_ids))
            | (to_station.isNotNull() & ~to_station.isin(*station_ids)),
        "suspicious_birthyear": (birthyear < rules.BIRTHYEAR_MIN) | (birthyear > rules.BIRTHYEAR_MAX),
        "duration_mismatch": F.abs(F.unix_timestamp(end) - F.unix_timestamp(start) - duration)
            > rules.DURATION_TOLERANCE_SECONDS,
    }
    reject = [c for c in rules.BATCH_CHECKS if rules.CHECKS[c][0] == "reject"]
    warn = [c for c in rules.BATCH_CHECKS if rules.CHECKS[c][0] == "warn"]

    # A comparison with a missing value gives NULL; NULL means "didn't fail".
    flagged = raw.select("*", *[F.coalesce(expr, F.lit(False)).alias(f"chk_{name}")
                                for name, expr in checks.items()])
    flagged = flagged.withColumn(
        "failed_checks", F.concat_ws(";", *[F.when(F.col(f"chk_{c}"), F.lit(c)) for c in reject])
    ).withColumn("rejected", F.col("failed_checks") != "")
    flagged.cache()  # also required before Spark lets us filter on _corrupt_record

    totals = flagged.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("rejected").cast("int")).alias("rejected"),
        F.sum(F.greatest(*[F.col(f"chk_{c}") for c in warn]).cast("int")).alias("warned"),
        *[F.sum(F.col(f"chk_{c}").cast("int")).alias(c) for c in rules.BATCH_CHECKS],
    ).first().asDict()
    rows_in, rejected = totals["rows"], totals["rejected"] or 0

    print(f"[quality] run {run_id}: {rows_in:,} rows read from the raw CSVs\n")
    print(f"{'check':<22} {'severity':<8} {'rows failed':>12} {'% failed':>9} {'alert at':>9}  status")
    results, alerts = [], []
    for c in rules.BATCH_CHECKS:
        failed = totals[c] or 0
        pct, st = rules.status(c, failed, rows_in)
        results.append({"run_id": run_id, "pipeline": "batch", "check_name": c,
                        "severity": rules.CHECKS[c][0], "description": rules.CHECKS[c][1],
                        "rows_checked": rows_in, "rows_failed": failed, "pct_failed": round(pct, 4),
                        "threshold_pct": rules.threshold_pct(c), "status": st})
        print(f"{c:<22} {rules.CHECKS[c][0]:<8} {failed:>12,} {pct:>8.4f}% {rules.threshold_pct(c):>8g}%  {st}")
        if st == "ALERT":
            alerts.append(c)

    print(f"\n[quality] rows in {rows_in:,} = passed {rows_in - rejected:,} + rejected {rejected:,}"
          f"  ({totals['warned'] or 0:,} passed rows have warnings)")

    bad = flagged.filter("rejected")
    if rejected:
        # Up to 5 per file, so a bad row added by hand shows up next to the real ones.
        per_file = Window.partitionBy("source_file").orderBy("row_seq")
        print("[quality] rejected rows (up to 5 per file):")
        for r in (bad.withColumn("n", F.row_number().over(per_file)).filter("n <= 5")
                  .orderBy("source_file", "row_seq").collect()):
            print(f"  {r.source_file}  trip {r.trip_id}  failed: {r.failed_checks}")
        by_file = bad.groupBy("source_file").count().orderBy("source_file").collect()
        print("[quality] rejected rows per file: "
              + ", ".join(f"{r.source_file} {r['count']:,}" for r in by_file))

    # Every rejected row, with its raw values, for the quarantine table. Written
    # as one plain CSV, row by row from the driver (so even millions of bad rows
    # don't have to fit in memory), not with Spark's writer: no temp folders or
    # renames for a file-syncing service to trip over.
    quarantine_cols = ["source_file", "failed_checks", "trip_id"]
    rows = (bad.orderBy("source_file", "row_seq")
            .select(*quarantine_cols,
                    F.to_json(F.struct(*RAW_COLUMNS, "_corrupt_record"),
                              {"ignoreNullFields": "false"}).alias("raw")))
    os.makedirs(OUTPUT, exist_ok=True)
    with open(QUARANTINE_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", *quarantine_cols, "raw"])
        for r in rows.toLocalIterator():
            w.writerow([run_id, *r])

    os.makedirs(RESULTS, exist_ok=True)
    with open(f"{RESULTS}/quality_checks.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=results[0].keys())
        w.writeheader()
        w.writerows(results)
    with open(f"{RESULTS}/quality_stage_counts.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "pipeline", "step", "stage", "rows_in", "rows_out", "rows_set_aside", "note"])
        w.writerow([run_id, "batch", 1, "raw CSV files", rows_in, rows_in, 0, "rows read from data/raw"])
        w.writerow([run_id, "batch", 2, "quality checks", rows_in, rows_in - rejected, rejected,
                    "rejected rows copied to the quarantine table"])
    print(f"[quality] results in {RESULTS}/quality_checks.csv, rejected rows in {QUARANTINE_CSV}")

    flagged.unpersist()
    spark.stop()

    if alerts:
        print(f"\n[quality] ALERT: {', '.join(alerts)} failed on more rows than the threshold allows.")
        if os.environ.get("QUALITY_FAIL_ON_ALERT", "1") != "0":
            print("[quality] Stopping the pipeline. Fix the data, or rerun with QUALITY_FAIL_ON_ALERT=0.")
            sys.exit(3)


if __name__ == "__main__":
    main()
