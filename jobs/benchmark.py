"""Part 1 deep part: same queries on four storage layouts, median of N runs.

Layouts: csv, parquet_flat, parquet_by_month, parquet_by_station.
Queries: Q1 (top stations + docks), Q2 (usertype x hour), June filter.
Each run builds a fresh DataFrame from disk (no caching) and forces the full
result to the driver, so the time includes reading and parsing the files.
"""
import csv
import os
import statistics
import sys

from pyspark.sql import functions as F

from common import LAYOUTS, RESULTS, STATIONS_PARQUET, Timer, spark_session
from questions import q1_top_stations, q2_usertype_by_hour

RUNS = int(os.environ.get("BENCH_RUNS", "5"))


def june_filter(spark, layout):
    """'Trips in June': the month layout can prune to one folder via the
    partition column; the others must filter on start_time."""
    from common import read_trips_csv
    if layout == "csv":
        df = read_trips_csv(spark)
    else:
        df = spark.read.parquet(LAYOUTS[layout])
    if layout == "parquet_by_month":
        df = df.filter(F.col("month") == 6)
    else:
        df = df.filter((F.col("start_time") >= F.lit("2017-06-01").cast("timestamp")) &
                       (F.col("start_time") < F.lit("2017-07-01").cast("timestamp")))
    return df.agg(F.count("*").alias("june_trips"), F.avg("tripduration").alias("avg_s"))


def main():
    layouts = sys.argv[1:] or list(LAYOUTS)
    spark = spark_session("divvy-benchmark")
    from common import read_trips
    stations = spark.read.parquet(STATIONS_PARQUET).cache()
    stations.count()

    queries = {
        "q1_top_stations": lambda l: q1_top_stations(read_trips(spark, l), stations),
        "q2_usertype_by_hour": lambda l: q2_usertype_by_hour(read_trips(spark, l)),
        "june_filter": lambda l: june_filter(spark, l),
    }

    # One untimed warm-up so JVM start-up and code generation don't land on the first layout.
    queries["q1_top_stations"]("parquet_flat").collect()

    results = []
    for layout in layouts:
        for qname, build in queries.items():
            times, answer = [], None
            for _ in range(RUNS):
                spark.catalog.clearCache()
                with Timer() as t:
                    answer = build(layout).collect()
                times.append(t.seconds)
            med = statistics.median(times)
            check = answer[0][0] if qname == "june_filter" else len(answer)
            print(f"[bench] {layout:<20} {qname:<20} median {med:6.2f} s  "
                  f"(runs: {', '.join(f'{x:.2f}' for x in times)})  result check: {check}")
            results.append({"layout": layout, "query": qname, "median_s": round(med, 3),
                            "min_s": round(min(times), 3), "max_s": round(max(times), 3),
                            "runs": RUNS, "result_check": check})

    os.makedirs(RESULTS, exist_ok=True)
    with open(f"{RESULTS}/benchmark.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=results[0].keys())
        w.writeheader()
        w.writerows(results)
    print(f"[bench] results written to {RESULTS}/benchmark.csv")
    spark.stop()


if __name__ == "__main__":
    main()
