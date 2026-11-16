# CPS 853 Team 7 — Divvy bike trips data platform

Dataset: Divvy trips, 2017 Q1 and Q2 (1,551,505 trips), and 582 stations. Source: https://divvybikes.com/system-data

## Start it (three commands)

```bash
# 1. Put the three CSVs in data/raw/:
#    Divvy_Trips_2017_Q1.csv  Divvy_Trips_2017_Q2.csv  Divvy_Stations_2017_Q1Q2.csv
cd cps853-divvy
docker compose build
docker compose run --rm spark-batch
```

That runs Part 1 from start to finish: converts the data, answers Q1 and Q2, then runs the benchmark. The full run takes about 5 minutes; the benchmark is most of that.
To skip the benchmark (e.g. for the checkpoint recording): `SKIP_BENCHMARK=1 docker compose run --rm spark-batch`.

| Folder | What's in it |
|---|---|
| `data/raw/` | the CSVs, as downloaded (not committed) |
| `data/lake/` | Parquet output: three trip layouts plus stations |
| `output/` | Q1 and Q2 answers as CSV, which Part 3 loads into PostgreSQL |
| `results/` | `storage_sizes.csv` and `benchmark.csv`, for the report |

## Part 1: Store and batch

| File | What it does |
|---|---|
| `jobs/common.py` | Spark session (local mode, 2 GB driver, Chicago time zone), schemas, layout paths |
| `jobs/convert.py` | CSV → Parquet. Reads every column as text, then casts, so bad values become NULL instead of shifting columns. Writes three layouts and counts rows in and out for each |
| `jobs/questions.py` | The Spark job that answers Q1 and Q2 from Parquet |
| `jobs/benchmark.py` | The deep part: the same three queries on all four layouts, 5 runs each, reporting the median |

### Answers (from the running job)

**Q1. Which ten stations start the most trips, and how busy are they for their number of docks?**

| Station | Docks | Trips started | Trips/dock/day |
|---|---|---|---|
| Streeter Dr & Grand Ave | 47 | 36,686 | 4.31 |
| Clinton St & Washington Blvd | 31 | 22,429 | 4.00 |
| Lake Shore Dr & Monroe St | 39 | 21,822 | 3.09 |
| Clinton St & Madison St | 31 | 19,098 | 3.40 |
| Canal St & Adams St | 47 | 18,105 | 2.13 |
| Lake Shore Dr & North Blvd | 39 | 17,729 | 2.51 |
| Theater on the Lake | 23 | 17,305 | 4.16 |
| Kingsbury St & Kinzie St | 27 | 14,853 | 3.04 |
| Michigan Ave & Oak St | 23 | 14,851 | 3.57 |
| Columbus Dr & Randolph St | 47 | 14,831 | 1.74 |

**Q2. Do annual members and day-pass riders ride at different times of day, and for how long?**

Yes, on both counts. Members' trips peak at 8 am (9.7% of their trips) and 5 pm (12.9%), which is a commuting pattern. Day-pass riders' trips peak in the early afternoon, from 1 to 4 pm (about 10–11% of their trips each hour). Day-pass trips are also about 2.5 times as long: a median of 23.0 minutes, against 9.4 minutes for members. The full 24-hour table is in `output/q2_usertype_by_hour.csv`.

### Deep part: storage format and partitioning

Run on 1,551,505 rows, Spark 3.5.3 in local mode on 2 cores. Each figure is the median of 5 runs, read fresh from disk each time.

| Layout | Size (MB) | Files | Write (s) | Q1 (s) | Q2 (s) | June filter (s) |
|---|---|---|---|---|---|---|
| CSV (baseline) | 242.3 | 2 | – | 1.27 | 2.95 | 1.88 |
| Parquet, flat | 41.5 | 2 | 3.3 | 0.33 | 1.39 | 0.25 |
| Parquet, by month | 42.7 | 6 | 4.4 | 0.37 | 1.50 | 0.21 |
| Parquet, by station | 54.1 | 582 | 10.6 | 3.65 | 5.39 | 3.31 |

Write times don't include parsing the CSV: the parsed data is cached once, then each layout is written from the cache.

**How this compares with what the proposal expected:**

1. **Parquet is at least 3× smaller than CSV: confirmed.** It is 5.8× smaller (242 MB → 41.5 MB), thanks to columnar compression and storing the timestamps as numbers instead of text.
2. **Q1 and Q2 run at least 2× faster on Parquet: confirmed.** Q1 is 3.8× faster, because it reads one column instead of parsing all twelve. Q2 is 2.1× faster. It gains less because the work is grouping the rows and computing a median for each group, not reading the file.
3. **Month partitions speed up the one-month filter: only slightly (0.25 → 0.21 s).** Flat Parquet already does almost as well. The source files are ordered by time, so each row group covers a narrow range of start times. Parquet stores the minimum and maximum of each row group, and Spark skips the groups that can't contain June. Partitioning would matter more for data that isn't already ordered by time, and for much bigger data, where listing the files and reading their footers starts to cost something.
4. **Station partitions are slower than month partitions: confirmed, and by a lot.** They are 10× slower on Q1 and 16× slower on the June filter. With 582 small files, the time goes on opening files and scheduling tasks rather than on reading data. The files are also 30% bigger, because compression works less well on small files.

**Decision:** Part 1 uses month-partitioned Parquet. It is about as fast as flat Parquet on every query here. It also matches the time-ordered data that Part 2 streams in, so new months can be added as new folders without rewriting the old ones.
