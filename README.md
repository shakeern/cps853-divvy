# CPS 853 Team 7 — Divvy bike trips data platform

Dataset: Divvy trips, 2017 Q1 and Q2 (1,551,505 trips), and 582 stations.
Source: https://divvybikes.com/system-data · Licence: https://divvybikes.com/data-license-agreement

## Start it

Needs Docker Desktop, running, with at least 8 GB of memory.

```bash
git clone https://github.com/shakeern/cps853-divvy.git
cd cps853-divvy
docker compose up --build
```

The first step downloads the three CSVs into `data/raw/` (skipped if they are already there). Then, in order:

1. **Part 4 + Part 1:** the quality checks run on every raw row, the rows that pass are converted to Parquet, and Spark answers Q1 and Q2.
2. **Part 3:** the answers, stations and check results are loaded into PostgreSQL.
3. **Part 2:** Kafka starts, and the producer replays 20,000 trips in start-time order (`STREAM_LIMIT=0` sends all of them).
4. **Parts 3 + 4:** the stream sink checks each message and writes it to PostgreSQL. It keeps running until you press Ctrl-C.

In a second terminal, once the stream is running:

| Command | What it shows |
|---|---|
| `docker compose exec postgres psql -U divvy -f /serve/queries.sql` | The three questions, answered by the database |
| `docker compose run --rm report` | Response times for Q1–Q3, then the quality and row-count report |
| `docker compose run --rm consumer` | Part 2's console consumer printing trips and hourly counts |
| `docker compose run --rm -e SKIP_BENCHMARK=0 spark-batch` | Part 1 again, plus the storage benchmark (several minutes) |
| `docker compose down -v` | Stop everything and wipe the database and Kafka topic, for a clean rerun |

Results for the report land in `results/` (benchmark, check results, response times, quality report) and `output/` (Q1 and Q2 answers, quarantined rows).

## Architecture

```
data/raw/*.csv ──► Part 4 batch checks ──► rejected rows ──► output/quarantine_batch.csv ─┐
                         │ rows that pass                                                  │
                         ▼                                                                 ▼
              Part 1: Parquet (data/lake/) ──► Spark Q1, Q2 ──► output/*.csv ──► PostgreSQL (Part 3)
                                                                                           ▲
data/raw/*.csv ──► Part 2 producer (sorted by start_time) ──► Kafka divvy-trips            │
                                                                │                          │
                                                                ▼                          │
                                         stream sink: Part 4 stream checks ──► trip_starts, station_hourly_counts (Q3)
                                                                └── failing messages ──► quarantine
```

## Part 1: Store and batch (owner: Shakeer)

| File | What it does |
|---|---|
| `jobs/common.py` | Spark session (local mode, 2 GB driver, Chicago time zone), schemas, layout paths |
| `jobs/convert.py` | Converts the rows that passed the quality checks to Parquet in three layouts, counting rows in and out of each |
| `jobs/questions.py` | The Spark job that answers Q1 and Q2 from month-partitioned Parquet |
| `jobs/benchmark.py` | The deep part: the same three queries on four layouts, 5 runs each, median reported |

**Q1. Which ten stations start the most trips, and how busy are they for their number of docks?**

| Station | Docks | Trips started | Trips/dock/day |
|---|---|---|---|
| Streeter Dr & Grand Ave | 47 | 36,684 | 4.31 |
| Clinton St & Washington Blvd | 31 | 22,429 | 4.00 |
| Lake Shore Dr & Monroe St | 39 | 21,820 | 3.09 |
| Clinton St & Madison St | 31 | 19,098 | 3.40 |
| Canal St & Adams St | 47 | 18,105 | 2.13 |
| Lake Shore Dr & North Blvd | 39 | 17,729 | 2.51 |
| Theater on the Lake | 23 | 17,305 | 4.16 |
| Kingsbury St & Kinzie St | 27 | 14,853 | 3.04 |
| Michigan Ave & Oak St | 23 | 14,851 | 3.57 |
| Columbus Dr & Randolph St | 47 | 14,831 | 1.74 |

**Q2. Do annual members and day-pass riders ride at different times of day, and for how long?**

Yes, on both counts. Members' trips peak at 8 am (9.7% of their trips) and 5 pm (12.9%), a commuting pattern. Day-pass riders peak from 1 to 4 pm (about 10–11% of their trips each hour). Day-pass trips are about 2.5 times as long: a median of 23.0 minutes, against 9.4 for members. The full table is in `output/q2_usertype_by_hour.csv`.

### Deep part: storage format and partitioning

Measured on Docker Desktop on Windows, Spark 3.5.3 in local mode. Each time is the median of 5 runs, read fresh from disk.

| Layout | Size (MB) | Files | Write (s) | Q1 (s) | Q2 (s) | June filter (s) |
|---|---|---|---|---|---|---|
| CSV (baseline) | 242.3 | 2 | – | 4.07 | 5.04 | 3.85 |
| Parquet, flat | 42.1 | 4 | 12.6 | 0.88 | 2.55 | 0.74 |
| Parquet, by month | 42.7 | 6 | 25.2 | 1.05 | 2.75 | 0.67 |
| Parquet, by station | 54.1 | 582 | 96.7 | 32.84 | 33.13 | 30.62 |

These timings were measured before the quality checks were added in front of Part 1, so the Parquet layouts held all 1,551,505 rows; they now hold the 1,551,494 that pass. Rerun the benchmark to refresh them.

Against what the proposal expected:

1. **Parquet at least 3× smaller than CSV: confirmed.** 5.8× smaller, from columnar compression and storing timestamps as numbers rather than text.
2. **Q1 and Q2 at least 2× faster on Parquet: confirmed for Q1, just missed for Q2.** Q1 is 4.6× faster, since it reads one column instead of parsing twelve. Q2 is 1.98× faster: most of its time goes on grouping rows and computing medians, which the file format doesn't speed up.
3. **Month partitions speed up the one-month filter: only slightly** (0.74 → 0.67 s). The source data is ordered by time, so each Parquet row group covers a narrow range of start times, and Spark already skips the groups that can't contain June using their min/max statistics.
4. **Station partitions slower than month partitions: confirmed, heavily.** About 31× slower on Q1, and 8× slower than raw CSV. With 582 small files, the time goes on opening files and scheduling tasks, not on reading data. Docker Desktop on Windows makes opening files in a shared folder especially slow; on Linux the gap was about 10×.

**Decision:** month-partitioned Parquet. It is as fast as flat Parquet here, and matches the time-ordered stream, so new months can be added as new folders.

## Part 2: Stream (owner: Abdirahman)

| File | What it does |
|---|---|
| `stream/producer.py` | Reads every trip, sorts by `start_time`, and sends oldest first to the `divvy-trips` topic, keyed by station. `--delay` sets a fixed gap between messages; `--speedup N` paces by event time instead (N seconds of trips per real second) |
| `stream/consumer.py` | Console consumer: prints each trip and keeps a running count of trips per station per hour |

The topic has 3 partitions. Keying by station sends all of a station's trips to the same partition, so they stay in order. Rows the producer can't read (for example a blank station ID) are sent as-is, first, so the stream checks catch them rather than the producer crashing.

## Part 3: Serve (owner: Haris)

PostgreSQL 16. `serve/schema.sql` creates the tables; `serve/queries.sql` answers the three questions; `serve/measure.py` times each one (50 runs after a warm-up) into `results/serve_response_times.csv`.

| Question | Table | Filled by |
|---|---|---|
| Q1 | `q1_top_stations` | `serve/load_batch.py`, from Spark's output |
| Q2 | `q2_usertype_by_hour` | `serve/load_batch.py`, from Spark's output |
| Q3 | `station_hourly_counts` (plus every trip in `trip_starts`) | `serve/stream_to_db.py`, from Kafka |

The stream sink saves each batch of trips and updates the hourly counts in one transaction, and only then commits its Kafka offsets. If it crashes and Kafka resends messages, the primary key on `trip_id` skips trips already stored, so no trip is counted twice.

## Part 4: Quality and monitoring (owner: Haris)

The rules live in `quality/rules.py`, shared by both pipelines.

| Check | Severity | Batch | Stream |
|---|---|---|---|
| `schema` — a value doesn't parse as its type | reject | ✓ | ✓ |
| `missing_required` — a required field is blank | reject | ✓ | ✓ |
| `impossible_range` — duration ≤ 0 or over 24 h, end before start, start outside Jan–Jun 2017 | reject | ✓ | ✓ |
| `invalid_category` — unknown user type or gender | reject | ✓ | |
| `duplicate_trip` — the same `trip_id` again | reject | ✓ | ✓ |
| `unknown_station` — a station not in the stations table | reject | ✓ | ✓ |
| `suspicious_birthyear` — outside 1920–2005 | warn | ✓ | |
| `duration_mismatch` — duration disagrees with end − start | warn | ✓ | |

Rejected rows go to the `quarantine` table with the checks they failed, instead of being dropped. Rows in and out are counted at every stage (`stage_counts`). A check failing on more than its threshold (0.1% for rejects) raises an alert; in the batch pipeline it also stops the run before bad data is converted (`QUALITY_FAIL_ON_ALERT=0` to carry on).

On the real data: 11 trips appear twice (exact copies), so 11 rows are quarantined and 1,551,494 pass. 558 rows have an unlikely birth year and are kept with a warning.

### Demo: a bad row added by hand

1. Add this line to the end of `data/raw/Divvy_Trips_2017_Q2.csv` (negative duration, station 9999 doesn't exist, start time early enough to be in the first 20,000 streamed trips):
   ```
   "99999999","1/1/2017 00:00:00","1/1/2017 00:10:00","1","-5","9999","Fake station","9999","Fake station","Subscriber","Male","1990"
   ```
2. `docker compose down -v`, then `docker compose up --build`.
3. Batch: the checks print the row as failing `impossible_range` and `unknown_station`, with the counts. Stream: the sink prints it as `REJECTED ... unknown_station`.
4. `docker compose run --rm report` shows it in the quarantine table, with rows in = rows out + set aside at every stage.
5. Delete the line afterwards.

## Team

| Member | Owns |
|---|---|
| Shakeer Nawaz | Part 1 and the deep part |
| Abdirahman Abdulahi | Part 2 |
| Haris Sujethan | Parts 3 and 4 |
