import argparse
import json
from collections import defaultdict
from datetime import datetime

from kafka import KafkaConsumer

TOPIC = "divvy-trips"
BOOTSTRAP_SERVERS = "localhost:9092"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="divvy-hourly-demo",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    # (station ID, hour) -> number of trips
    hourly_counts = defaultdict(int)
    seen_trip_ids = set()
    processed = 0
    duplicates = 0

    print(f"[consumer] Listening to topic '{TOPIC}'...")

    try:
        for message in consumer:
            event = message.value

            trip_id = str(event["trip_id"])

            if trip_id in seen_trip_ids:
                duplicates += 1
                continue

            seen_trip_ids.add(trip_id)

            station_id = int(event["from_station_id"])

            # Divvy CSV uses timestamps like "3/31/2017 23:59:07"
            start_time = datetime.strptime(event["start_time"], "%m/%d/%Y %H:%M:%S")

            hour = start_time.strftime("%Y-%m-%d %H:00")
            key = (station_id, hour)

            hourly_counts[key] += 1
            processed += 1

            print(
                f"[consumer] Trip {event['trip_id']} | "
                f"Station {station_id} | "
                f"Hour {hour} | "
                f"Count: {hourly_counts[key]}"
            )

            if processed >= args.limit:
                break

    except KeyboardInterrupt:
        print("\n[consumer] Stopped by user.")

    finally:
        print("\n=== Hourly station counts ===")

        for (station_id, hour), count in sorted(hourly_counts.items()):
            print(f"Station {station_id} | {hour} | {count} trips")

        print(f"\n[consumer] Processed {processed} events.")
        print(f"[consumer] Skipped {duplicates} duplicate events.")
        consumer.close()


if __name__ == "__main__":
    main()
