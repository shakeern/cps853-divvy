import argparse
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer

TOPIC = "divvy-trips"
BOOTSTRAP_SERVERS = "localhost:9092"

DATA_FILES = [
    Path("data/raw/Divvy_Trips_2017_Q1.csv"),
    Path("data/raw/Divvy_Trips_2017_Q2.csv"),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.05)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    count = 0

    for path in DATA_FILES:
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)

            for row in reader:
                event = {
                    "trip_id": row["trip_id"],
                    "start_time": row["start_time"],
                    "from_station_id": int(row["from_station_id"]),
                }

                producer.send(
                    TOPIC,
                    key=event["from_station_id"],
                    value=event,
                )

                count += 1

                if count <= 10 or count % 1000 == 0:
                    print(f"[producer] Sent {count}: {event}")

                if args.delay > 0:
                    time.sleep(args.delay)

                if count >= args.limit:
                    producer.flush()
                    print(f"[producer] Finished sending {count} events.")
                    producer.close()
                    return

    producer.flush()
    producer.close()
    print(f"[producer] Finished sending {count} events.")


if __name__ == "__main__":
    main()
