"""Part 1, step 2: the Spark job that answers Questions 1 and 2 from the stored data.

Q1  "Which ten stations start the most trips, and how busy are they for their
     number of docks?"
Q2  "Do annual members and day-pass riders ride at different times of day,
     and for how long?"

The functions take a trips DataFrame so benchmark.py can run the exact same
queries on every storage layout. Run directly, this reads the month-partitioned
Parquet, prints both answers and saves them to output/ for Part 3 to load.
"""
import os
import sys

from pyspark.sql import functions as F

from common import OUTPUT, STATIONS_PARQUET, Timer, read_trips, spark_session

DAYS_IN_DATA = 181  # 2017-01-01 .. 2017-06-30


def q1_top_stations(trips, stations, n=10):
    starts = trips.groupBy("from_station_id").agg(F.count("*").alias("trips_started"))
    return (starts.join(stations, starts.from_station_id == stations.station_id, "left")
            .select(
                F.col("from_station_id").alias("station_id"),
                "station_name",
                "dpcapacity",
                "trips_started",
                F.round(F.col("trips_started") / F.col("dpcapacity"), 1).alias("trips_per_dock"),
                F.round(F.col("trips_started") / F.col("dpcapacity") / DAYS_IN_DATA, 2)
                 .alias("trips_per_dock_per_day"))
            .orderBy(F.desc("trips_started"))
            .limit(n))


def q2_usertype_by_hour(trips):
    # "Dependent" is 4 rows in Q1; keep only the two rider types the question is about.
    t = trips.filter(F.col("usertype").isin("Subscriber", "Customer"))
    return (t.groupBy("usertype", F.hour("start_time").alias("hour"))
            .agg(F.count("*").alias("trips"),
                 F.round(F.avg("tripduration") / 60, 1).alias("avg_minutes"),
                 F.round(F.percentile_approx("tripduration", 0.5) / 60, 1).alias("median_minutes"))
            .orderBy("usertype", "hour"))


def q2_summary(trips):
    t = trips.filter(F.col("usertype").isin("Subscriber", "Customer"))
    return (t.groupBy("usertype")
            .agg(F.count("*").alias("trips"),
                 F.round(F.avg("tripduration") / 60, 1).alias("avg_minutes"),
                 F.round(F.percentile_approx("tripduration", 0.5) / 60, 1).alias("median_minutes"))
            .orderBy("usertype"))


def add_share(q2):
    """Share of each rider type's trips that start in each hour, so the two
    curves are comparable despite members taking ~4x more trips."""
    from pyspark.sql import Window
    w = Window.partitionBy("usertype")
    return q2.withColumn("pct_of_type", F.round(100 * F.col("trips") / F.sum("trips").over(w), 1))


def main(layout="parquet_by_month"):
    spark = spark_session("divvy-questions")
    trips = read_trips(spark, layout)
    stations = spark.read.parquet(STATIONS_PARQUET)
    print(f"[questions] reading layout: {layout}  ({trips.count():,} trips)")

    with Timer() as t1:
        q1 = q1_top_stations(trips, stations).toPandas()
    print(f"\nQ1  Which ten stations start the most trips, and how busy are they for their docks?"
          f"   ({t1.seconds:.2f} s)")
    print(q1.to_string(index=False))

    with Timer() as t2:
        q2 = add_share(q2_usertype_by_hour(trips)).toPandas()
        summary = q2_summary(trips).toPandas()
    print(f"\nQ2  Do annual members and day-pass riders ride at different times of day, and for how long?"
          f"   ({t2.seconds:.2f} s)")
    print(summary.to_string(index=False))
    wide = q2.pivot(index="hour", columns="usertype", values=["pct_of_type", "median_minutes"])
    wide.columns = [f"{v}_{u}".replace("Customer", "daypass").replace("Subscriber", "member")
                    for v, u in wide.columns]
    print("\nBy hour of day (share of that rider type's trips, and median trip length):")
    print(wide.reset_index().to_string(index=False))

    os.makedirs(OUTPUT, exist_ok=True)
    q1.to_csv(f"{OUTPUT}/q1_top_stations.csv", index=False)
    q2.to_csv(f"{OUTPUT}/q2_usertype_by_hour.csv", index=False)
    print(f"\n[questions] answers saved to {OUTPUT}/ for Part 3 (Serve)")
    spark.stop()


if __name__ == "__main__":
    main(*sys.argv[1:])
