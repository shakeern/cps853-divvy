"""Part 3, Serve: load the batch answers (Q1, Q2) and the stations into PostgreSQL,
along with the Part 4 batch quality results.

Reads what Part 1 (and its Part 4 quality stage) wrote:
  data/raw/Divvy_Stations_2017_Q1Q2.csv
  output/q1_top_stations.csv
  output/q2_usertype_by_hour.csv
  output/quarantine_batch.csv           rows rejected by the quality checks
  results/quality_checks.csv            result of each check
  results/quality_stage_counts.csv      rows in/out of the first two stages
  results/storage_sizes.csv             rows written to Parquet

Everything is replaced in one transaction, so running it again after a new
Spark run is safe, and anyone querying sees either the old answers or the
new ones, never half of each. Stream quality results are left alone.
"""
import csv
from datetime import datetime
from pathlib import Path

from db import apply_schema, connect

ROOT = Path(__file__).resolve().parent.parent
STATIONS_CSV = ROOT / "data/raw/Divvy_Stations_2017_Q1Q2.csv"
Q1_CSV = ROOT / "output/q1_top_stations.csv"
Q2_CSV = ROOT / "output/q2_usertype_by_hour.csv"
QUARANTINE_CSV = ROOT / "output/quarantine_batch.csv"
CHECKS_CSV = ROOT / "results/quality_checks.csv"
STAGES_CSV = ROOT / "results/quality_stage_counts.csv"
SIZES_CSV = ROOT / "results/storage_sizes.csv"


def read_csv(path: Path, required=True) -> list[dict]:
    if not path.exists():
        if required:
            raise SystemExit(f"[load_batch] Missing {path.relative_to(ROOT)}: run Part 1 first "
                             "(docker compose run --rm spark-batch).")
        return []
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def whole(value: str) -> int:
    """An integer from a CSV cell. Accepts "7" and "7.0": when a column has a blank
    cell, pandas (which writes Part 1's answers) writes the whole column as decimals."""
    return int(float(value))


def none_if_blank(value: str):
    return value if value not in ("", None) else None


def complete(rows, keys, label):
    """Split answer rows into those with every key filled in, and the rest.

    Part 1 converts every raw row, including ones the quality checks rejected.
    A rejected row with, say, an unreadable start_time ends up in Q2 under a
    blank hour. Such a row can't go into a table keyed on hour, so it is set
    aside here, reported, and counted in stage_counts, instead of failing the load.
    """
    good = [r for r in rows if all(r[k] not in ("", None) for k in keys)]
    bad = len(rows) - len(good)
    if bad:
        print(f"[load_batch] WARNING: {bad} row(s) of {label} have a blank {' or '.join(keys)} "
              f"(from raw rows the quality checks rejected) and are set aside, not loaded.")
    return good


def copy_rows(cur, sql, rows):
    with cur.copy(sql) as copy:
        for row in rows:
            copy.write_row(row)


def batch_stages(run_id, stage_rows, q2):
    """Rows in and out of every batch stage, from the files each stage wrote.

    Returns the stage rows, and a note if Part 1 didn't run after these checks
    (the pipeline stopped on an alert), in which case only the check stages count.
    """
    stages = [(run_id, int(r["step"]), r["stage"], int(r["rows_in"]), int(r["rows_out"]),
               int(r["rows_set_aside"]), r["note"]) for r in stage_rows]
    passed_rows = stages[-1][4]  # rows out of the quality-check stage

    # Did convert.py run after the checks? If the checks raised an alert and
    # stopped the pipeline, storage_sizes.csv is from an older run.
    if not SIZES_CSV.exists() or SIZES_CSV.stat().st_mtime < CHECKS_CSV.stat().st_mtime:
        return stages, "Parquet stage not counted: Part 1 hasn't converted the data since these checks ran."
    parquet_rows = next(int(r["rows"]) for r in read_csv(SIZES_CSV) if r["layout"] == "parquet_by_month")
    used_by_q2 = sum(whole(r["trips"]) for r in q2)
    stages += [
        (run_id, 3, "Parquet, by month (Part 1)", passed_rows, parquet_rows, passed_rows - parquet_rows,
         "Part 1 converts only the rows that passed the checks"),
        (run_id, 4, "Spark Q2 (Part 1)", parquet_rows, used_by_q2, parquet_rows - used_by_q2,
         "set aside = 'Dependent' riders, left out of Q2 by design (it compares members and day-pass riders)"),
    ]
    return stages, None


def main():
    stations = [
        (int(r["id"]), r["name"], r["city"], float(r["latitude"]), float(r["longitude"]),
         int(r["dpcapacity"]), datetime.strptime(r["online_date"], "%m/%d/%Y %H:%M:%S"))
        for r in read_csv(STATIONS_CSV)
    ]
    q1_raw = read_csv(Q1_CSV)
    q1 = [
        (rank, whole(r["station_id"]), r["station_name"], whole(r["dpcapacity"]),
         whole(r["trips_started"]), r["trips_per_dock"], r["trips_per_dock_per_day"])
        for rank, r in enumerate(complete(q1_raw, ["station_id", "dpcapacity", "trips_started"],
                                          "q1_top_stations.csv"), start=1)
    ]
    q2_raw = read_csv(Q2_CSV)
    q2 = [
        (r["usertype"], whole(r["hour"]), whole(r["trips"]), none_if_blank(r["avg_minutes"]),
         none_if_blank(r["median_minutes"]), none_if_blank(r["pct_of_type"]))
        for r in complete(q2_raw, ["usertype", "hour", "trips"], "q2_usertype_by_hour.csv")
    ]

    checks = read_csv(CHECKS_CSV, required=False)
    stage_rows = read_csv(STAGES_CSV, required=False)
    quarantine = [(r["run_id"], "batch", r["failed_checks"].split(";"),
                   none_if_blank(r["trip_id"]), r["raw"], r["source_file"])
                  for r in read_csv(QUARANTINE_CSV, required=False)]
    have_quality = bool(checks and stage_rows)
    if not have_quality:
        print("[load_batch] No Part 4 quality results in results/: rerun Part 1 "
              "(docker compose run --rm spark-batch). Loading the answers only.")

    with connect() as conn:
        apply_schema(conn)
        with conn.transaction(), conn.cursor() as cur:
            cur.execute("TRUNCATE q1_top_stations, q2_usertype_by_hour, stations")
            copy_rows(cur, "COPY stations (station_id, station_name, city, latitude, longitude, "
                           "dpcapacity, online_date) FROM STDIN", stations)
            copy_rows(cur, "COPY q1_top_stations (rank, station_id, station_name, dpcapacity, "
                           "trips_started, trips_per_dock, trips_per_dock_per_day) FROM STDIN", q1)
            copy_rows(cur, "COPY q2_usertype_by_hour (usertype, hour, trips, avg_minutes, "
                           "median_minutes, pct_of_type) FROM STDIN", q2)

            if have_quality:
                run_id = checks[0]["run_id"]
                # Keep only the latest batch run; stream runs are untouched.
                cur.execute("DELETE FROM quality_checks WHERE pipeline = 'batch'")
                cur.execute("DELETE FROM stage_counts WHERE pipeline = 'batch'")
                cur.execute("DELETE FROM quarantine WHERE source = 'batch'")
                copy_rows(cur, "COPY quality_checks (run_id, pipeline, check_name, severity, description, "
                               "rows_checked, rows_failed, pct_failed, threshold_pct, status) FROM STDIN",
                          [(c["run_id"], c["pipeline"], c["check_name"], c["severity"], c["description"],
                            c["rows_checked"], c["rows_failed"], c["pct_failed"], c["threshold_pct"],
                            c["status"]) for c in checks])
                copy_rows(cur, "COPY quarantine (run_id, source, failed_checks, trip_id, raw, source_file) "
                               "FROM STDIN", quarantine)
                for c in checks:
                    if c["status"] == "ALERT":
                        cur.execute(
                            "INSERT INTO quality_alerts (run_id, pipeline, check_name, rows_checked, "
                            "rows_failed, pct_failed, threshold_pct, message) "
                            "VALUES (%s, 'batch', %s, %s, %s, %s, %s, %s) "
                            "ON CONFLICT (run_id, check_name) DO NOTHING",
                            (c["run_id"], c["check_name"], c["rows_checked"], c["rows_failed"],
                             c["pct_failed"], c["threshold_pct"],
                             f"{c['check_name']}: {c['rows_failed']} of {c['rows_checked']} rows failed "
                             f"({float(c['pct_failed']):.4f}%), over the {c['threshold_pct']}% threshold"))

                stages, skipped = batch_stages(run_id, stage_rows, q2_raw)
                if skipped:
                    print(f"[load_batch] {skipped}")
                # The last stage: rows in each file vs rows now in each table.
                step = max(s[1] for s in stages)
                for table, rows_in in [("stations", len(stations)), ("q1_top_stations", len(q1_raw)),
                                       ("q2_usertype_by_hour", len(q2_raw)), ("quarantine", len(quarantine))]:
                    where = " WHERE source = 'batch'" if table == "quarantine" else ""
                    rows_out = cur.execute(f"SELECT count(*) FROM {table}{where}").fetchone()[0]
                    step += 1
                    note = "rows in the file vs rows in the table"
                    if rows_in != rows_out:
                        note += "; set aside = rows with a blank key, from raw rows the checks rejected"
                    stages.append((run_id, step, f"PostgreSQL {table}", rows_in, rows_out,
                                   rows_in - rows_out, note))
                copy_rows(cur, "COPY stage_counts (run_id, pipeline, step, stage, rows_in, rows_out, "
                               "rows_set_aside, note) FROM STDIN",
                          [(s[0], "batch", *s[1:]) for s in stages])

    print(f"[load_batch] stations: {len(stations)} rows, q1_top_stations: {len(q1)} rows, "
          f"q2_usertype_by_hour: {len(q2)} rows")
    if have_quality:
        print(f"[load_batch] quality: {len(checks)} check results, {len(quarantine)} quarantined rows, "
              f"{len(stages)} stage counts (run {run_id})")


if __name__ == "__main__":
    main()
