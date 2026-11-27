"""Part 1, step 1: convert the raw Divvy CSVs into Parquet.

Writes three Parquet layouts of the same trips (for the deep-part comparison)
plus the stations table, and reports rows in/out, size on disk and write time.
"""
import csv
import glob
import os

from pyspark.sql import functions as F

from common import (CHECKED_TRIPS, LAYOUTS, RESULTS, STATIONS_CSV, STATIONS_PARQUET, TRIPS_CSV,
                    Timer, count_data_files, dir_size_mb, read_stations_csv,
                    parse_trips, read_trips_csv, reset_dir, spark_session)


def main():
    spark = spark_session("divvy-convert")
    os.makedirs(RESULTS, exist_ok=True)

    csv_files = sorted(glob.glob(TRIPS_CSV))
    csv_mb = sum(dir_size_mb(f) for f in csv_files)

    csv_rows = read_trips_csv(spark).count()
    print(f"[convert] rows in the raw CSV: {csv_rows:,} ({len(csv_files)} files, {csv_mb:.1f} MB)")

    # Convert only the rows that passed the Part 4 checks. Without them (the
    # checks were skipped), fall back to the raw CSV.
    if os.path.isdir(CHECKED_TRIPS):
        source = parse_trips(spark.read.parquet(CHECKED_TRIPS))
        print(f"[convert] reading the rows that passed the quality checks ({CHECKED_TRIPS})")
    else:
        source = read_trips_csv(spark)
        print("[convert] WARNING: no quality-checked rows found; converting the raw CSV unchecked")
    trips = source.withColumn("month", F.month("start_time"))
    trips.cache()
    rows_in = trips.count()
    print(f"[convert] rows to convert: {rows_in:,} ({csv_rows - rows_in:,} rejected by the checks)")

    writers = {
        # One dataset, a few large files.
        "parquet_flat": lambda df, p: df.drop("month").coalesce(4).write.parquet(p),
        # month=1 ... month=6 folders, one file each.
        "parquet_by_month": lambda df, p: df.repartition("month").write.partitionBy("month").parquet(p),
        # ~580 from_station_id=N folders, one small file each (over-partitioning test).
        "parquet_by_station": lambda df, p: df.drop("month").repartition("from_station_id")
                                              .write.partitionBy("from_station_id").parquet(p),
    }

    rows = [{"layout": "csv", "size_mb": round(csv_mb, 1), "files": len(csv_files),
             "write_s": "", "rows": csv_rows}]
    for name, write in writers.items():
        path = LAYOUTS[name]
        reset_dir(path)
        with Timer() as t:
            write(trips, path)
        rows_out = spark.read.parquet(path).count()
        size = dir_size_mb(path)
        files = count_data_files(path)
        status = "OK" if rows_out == rows_in else "MISMATCH"
        print(f"[convert] {name:<20} rows out {rows_out:,} ({status})  "
              f"{size:7.1f} MB  {files:4d} files  written in {t.seconds:5.1f} s  "
              f"({csv_mb / size:.1f}x smaller than CSV)")
        rows.append({"layout": name, "size_mb": round(size, 1), "files": files,
                     "write_s": round(t.seconds, 1), "rows": rows_out})
    trips.unpersist()

    stations = read_stations_csv(spark)
    reset_dir(STATIONS_PARQUET)
    stations.coalesce(1).write.parquet(STATIONS_PARQUET)
    print(f"[convert] stations: {stations.count():,} rows -> {STATIONS_PARQUET} "
          f"({dir_size_mb(STATIONS_CSV) * 1000:.0f} KB CSV -> {dir_size_mb(STATIONS_PARQUET) * 1000:.0f} KB Parquet)")

    with open(f"{RESULTS}/storage_sizes.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"[convert] sizes written to {RESULTS}/storage_sizes.csv")
    spark.stop()


if __name__ == "__main__":
    main()
