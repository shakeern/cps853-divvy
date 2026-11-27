"""Part 3, Serve: ask PostgreSQL the three questions and measure each response time.

The SQL is read from queries.sql, so the queries timed here are exactly the
ones shown in the demo. Each query runs once to warm up, then --runs times;
the time is the round trip as a user of the database sees it: send the query,
wait, and fetch every row of the answer.

Writes results/serve_response_times.csv for the report.
"""
import argparse
import csv
import statistics
import time
from pathlib import Path

from db import connect

HERE = Path(__file__).resolve().parent
QUERIES_SQL = HERE / "queries.sql"
OUT_CSV = HERE.parent / "results/serve_response_times.csv"
NAMES = ["Q1 top ten stations", "Q2 member vs day-pass by hour", "Q3 trips per station this hour"]


def load_queries() -> list[str]:
    """The SELECT statements in queries.sql, without psql commands or comments."""
    lines = [l for l in QUERIES_SQL.read_text().splitlines()
             if not l.lstrip().startswith(("\\", "--"))]
    return [q.strip() for q in "\n".join(lines).split(";") if q.strip()]


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--runs", type=int, default=50)
    args = parser.parse_args()

    queries = load_queries()
    assert len(queries) == len(NAMES), f"expected {len(NAMES)} queries in queries.sql, found {len(queries)}"

    results = []
    with connect() as conn:
        for name, sql in zip(NAMES, queries):
            rows = conn.execute(sql).fetchall()  # warm-up
            times_ms = []
            for _ in range(args.runs):
                start = time.perf_counter()
                rows = conn.execute(sql).fetchall()
                times_ms.append((time.perf_counter() - start) * 1000)
            times_ms.sort()
            results.append({
                "question": name,
                "rows_returned": len(rows),
                "runs": args.runs,
                "median_ms": round(statistics.median(times_ms), 2),
                "p95_ms": round(times_ms[max(0, int(0.95 * len(times_ms)) - 1)], 2),
                "min_ms": round(times_ms[0], 2),
                "max_ms": round(times_ms[-1], 2),
            })
        stored_trips = conn.execute("SELECT count(*) FROM trip_starts").fetchone()[0]

    print(f"Response times from PostgreSQL, {args.runs} runs each after one warm-up "
          f"({stored_trips:,} streamed trips stored):\n")
    print(f"{'question':<34} {'rows':>5} {'median ms':>10} {'p95 ms':>8} {'max ms':>8}")
    for r in results:
        print(f"{r['question']:<34} {r['rows_returned']:>5} {r['median_ms']:>10.2f} "
              f"{r['p95_ms']:>8.2f} {r['max_ms']:>8.2f}")

    OUT_CSV.parent.mkdir(exist_ok=True)
    with OUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[*results[0], "streamed_trips_stored"])
        writer.writeheader()
        for r in results:
            writer.writerow({**r, "streamed_trips_stored": stored_trips})
    print(f"\n[measure] saved to {OUT_CSV.relative_to(HERE.parent)}")


if __name__ == "__main__":
    main()
