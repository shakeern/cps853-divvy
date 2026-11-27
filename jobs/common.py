"""Shared Spark session, schemas and paths for Part 1 (Store and batch)."""
import os
import shutil
import time

from pyspark.sql import SparkSession, functions as F, types as T

DATA = os.environ.get("DATA_DIR", "/app/data")
RAW = f"{DATA}/raw"
LAKE = f"{DATA}/lake"
OUTPUT = os.environ.get("OUTPUT_DIR", "/app/output")
RESULTS = os.environ.get("RESULTS_DIR", "/app/results")

TRIPS_CSV = f"{RAW}/Divvy_Trips_2017_Q*.csv"
STATIONS_CSV = f"{RAW}/Divvy_Stations_2017_Q1Q2.csv"

# The four layouts compared in the deep part.
LAYOUTS = {
    "csv": None,                                      # raw files, as downloaded
    "parquet_flat": f"{LAKE}/trips_parquet_flat",      # one unpartitioned dataset
    "parquet_by_month": f"{LAKE}/trips_parquet_by_month",
    "parquet_by_station": f"{LAKE}/trips_parquet_by_station",
}
STATIONS_PARQUET = f"{LAKE}/stations_parquet"
# Written by quality/batch_checks.py: the raw rows (still text) that passed
# every reject check. convert.py builds the Parquet layouts from these.
CHECKED_TRIPS = f"{LAKE}/trips_checked"

TS_FMT = "M/d/yyyy H:mm:ss"

# Read every column as text first, so a bad value becomes NULL instead of
# silently shifting columns; types are applied in parse_trips().
TRIPS_RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in [
    "trip_id", "start_time", "end_time", "bikeid", "tripduration",
    "from_station_id", "from_station_name", "to_station_id", "to_station_name",
    "usertype", "gender", "birthyear"]])

STATIONS_RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in [
    "id", "name", "city", "latitude", "longitude", "dpcapacity", "online_date"]])


def spark_session(app="divvy-part1"):
    spark = (SparkSession.builder.appName(app)
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "America/Chicago")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_trips_csv(spark):
    raw = spark.read.csv(TRIPS_CSV, header=True, schema=TRIPS_RAW_SCHEMA)
    return parse_trips(raw)


def parse_trips(raw):
    return (raw.select(
        F.col("trip_id").cast("long").alias("trip_id"),
        F.to_timestamp("start_time", TS_FMT).alias("start_time"),
        F.to_timestamp("end_time", TS_FMT).alias("end_time"),
        F.col("bikeid").cast("int").alias("bikeid"),
        F.col("tripduration").cast("int").alias("tripduration"),
        F.col("from_station_id").cast("int").alias("from_station_id"),
        "from_station_name",
        F.col("to_station_id").cast("int").alias("to_station_id"),
        "to_station_name",
        "usertype",
        "gender",
        F.col("birthyear").cast("int").alias("birthyear"),
    ))


def read_stations_csv(spark):
    raw = spark.read.csv(STATIONS_CSV, header=True, schema=STATIONS_RAW_SCHEMA)
    return raw.select(
        F.col("id").cast("int").alias("station_id"),
        F.col("name").alias("station_name"),
        "city",
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("dpcapacity").cast("int").alias("dpcapacity"),
        F.to_timestamp("online_date", TS_FMT).alias("online_date"),
    )


def read_trips(spark, layout):
    """Return the trips DataFrame for a layout, with identical columns for all four."""
    if layout == "csv":
        return read_trips_csv(spark)
    df = spark.read.parquet(LAYOUTS[layout])
    return df.drop("month") if "month" in df.columns else df


def dir_size_mb(path):
    if os.path.isfile(path):
        return os.path.getsize(path) / 1e6
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total / 1e6


def count_data_files(path):
    n = 0
    for _, _, files in os.walk(path):
        n += sum(1 for f in files if f.endswith(".parquet"))
    return n


def reset_dir(path):
    shutil.rmtree(path, ignore_errors=True)


class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.seconds = time.perf_counter() - self.t0
