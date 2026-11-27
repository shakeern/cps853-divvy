-- Parts 3 and 4: the PostgreSQL tables behind all three questions, and the
-- quality checks, row counts, quarantine and alerts.
-- Safe to run again: every statement is IF NOT EXISTS / OR REPLACE.
-- Both loaders (load_batch.py, stream_to_db.py) apply this file on start.
-- Times are Chicago local time, as in the source data, so they are stored
-- as plain TIMESTAMP (no time zone).

-- Reference data: one row per Divvy station (Divvy_Stations_2017_Q1Q2.csv).
CREATE TABLE IF NOT EXISTS stations (
    station_id   INTEGER PRIMARY KEY,
    station_name TEXT    NOT NULL,
    city         TEXT,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    dpcapacity   INTEGER,
    online_date  TIMESTAMP
);

-- Q1 (batch, from Spark): the ten stations that start the most trips.
CREATE TABLE IF NOT EXISTS q1_top_stations (
    rank                   SMALLINT PRIMARY KEY,
    station_id             INTEGER NOT NULL REFERENCES stations (station_id),
    station_name           TEXT    NOT NULL,
    dpcapacity             INTEGER,
    trips_started          INTEGER NOT NULL,
    trips_per_dock         NUMERIC(8, 1),
    trips_per_dock_per_day NUMERIC(6, 2),
    loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Q2 (batch, from Spark): trips by rider type and hour of day.
CREATE TABLE IF NOT EXISTS q2_usertype_by_hour (
    usertype       TEXT     NOT NULL CHECK (usertype IN ('Subscriber', 'Customer')),
    hour           SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
    trips          INTEGER  NOT NULL,
    avg_minutes    NUMERIC(6, 1),
    median_minutes NUMERIC(6, 1),
    pct_of_type    NUMERIC(4, 1),
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (usertype, hour)
);

-- Q3 (stream, from Kafka). Every trip the stream delivers, once.
-- The primary key on trip_id is what makes a replayed or redelivered
-- message harmless: the second insert of the same trip does nothing.
-- No foreign key to stations: a live stream must not stop on a station the
-- reference table hasn't heard of yet.
CREATE TABLE IF NOT EXISTS trip_starts (
    trip_id         BIGINT    PRIMARY KEY,
    from_station_id INTEGER   NOT NULL,
    start_time      TIMESTAMP NOT NULL,
    kafka_partition INTEGER   NOT NULL,
    kafka_offset    BIGINT    NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Arrival order. The Divvy files run newest trip first, so the stream
    -- moves backwards in time: "this hour" means the hour of the trip that
    -- arrived last, not the latest start_time.
    ingest_seq      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE
);

-- Q3 running count, kept up to date in the same transaction as each insert
-- into trip_starts, so it never disagrees with it. Reading "this hour" is
-- then a lookup by primary key, not a GROUP BY over every trip.
CREATE TABLE IF NOT EXISTS station_hourly_counts (
    station_id INTEGER   NOT NULL,
    hour_start TIMESTAMP NOT NULL,
    trips      INTEGER   NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (hour_start, station_id)
);

-- Check for Part 4: recount from trip_starts. It must match the table above.
CREATE OR REPLACE VIEW station_hourly_recount AS
SELECT from_station_id AS station_id,
       date_trunc('hour', start_time) AS hour_start,
       count(*)::INTEGER AS trips
FROM trip_starts
GROUP BY 1, 2;

-- ---------------------------------------------------------------------------
-- Part 4, Quality and monitoring
-- Batch rows come from quality/batch_checks.py (loaded by load_batch.py, which
-- keeps only the latest batch run). Stream rows are written by
-- stream_to_db.py, one run_id per run of the sink, in the same transaction as
-- the trips they describe.

-- One row per check per run: how many rows it looked at, how many failed.
CREATE TABLE IF NOT EXISTS quality_checks (
    run_id        TEXT    NOT NULL,
    pipeline      TEXT    NOT NULL CHECK (pipeline IN ('batch', 'stream')),
    check_name    TEXT    NOT NULL,
    severity      TEXT    NOT NULL CHECK (severity IN ('reject', 'warn')),
    description   TEXT,
    rows_checked  BIGINT  NOT NULL,
    rows_failed   BIGINT  NOT NULL,
    pct_failed    NUMERIC(9, 4),
    threshold_pct NUMERIC(9, 4),
    status        TEXT    NOT NULL CHECK (status IN ('PASS', 'FAIL', 'WARN', 'ALERT', 'SKIPPED')),
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);

-- Rows in and rows out at every stage of both pipelines.
-- rows_in = rows_out + rows_set_aside at every stage; the report checks it.
CREATE TABLE IF NOT EXISTS stage_counts (
    run_id         TEXT     NOT NULL,
    pipeline       TEXT     NOT NULL CHECK (pipeline IN ('batch', 'stream')),
    step           SMALLINT NOT NULL,
    stage          TEXT     NOT NULL,
    rows_in        BIGINT   NOT NULL,
    rows_out       BIGINT   NOT NULL,
    rows_set_aside BIGINT   NOT NULL DEFAULT 0,
    note           TEXT,
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stage)
);

-- Bad rows set aside instead of dropped: what failed, and the row as it arrived.
CREATE TABLE IF NOT EXISTS quarantine (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          TEXT   NOT NULL,
    source          TEXT   NOT NULL CHECK (source IN ('batch', 'stream')),
    failed_checks   TEXT[] NOT NULL,
    trip_id         TEXT,
    raw             TEXT   NOT NULL,
    source_file     TEXT,            -- batch: which CSV
    kafka_partition INTEGER,         -- stream: where in the topic
    kafka_offset    BIGINT,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- A redelivered bad message is quarantined once. The raw text is part of the
-- key, so a new message at a reused offset (after the topic is recreated) is
-- still caught.
CREATE UNIQUE INDEX IF NOT EXISTS quarantine_stream_message
    ON quarantine (kafka_partition, kafka_offset, md5(raw)) WHERE source = 'stream';

-- Raised when a check fails on more rows than its threshold allows.
CREATE TABLE IF NOT EXISTS quality_alerts (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        TEXT   NOT NULL,
    pipeline      TEXT   NOT NULL CHECK (pipeline IN ('batch', 'stream')),
    check_name    TEXT   NOT NULL,
    rows_checked  BIGINT NOT NULL,
    rows_failed   BIGINT NOT NULL,
    pct_failed    NUMERIC(9, 4),
    threshold_pct NUMERIC(9, 4),
    message       TEXT   NOT NULL,
    raised_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, check_name)
);
