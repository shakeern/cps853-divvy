"""Download the Divvy 2017 Q1/Q2 data into data/raw/, if it isn't there yet.

The first step of `docker compose up`. Divvy publishes Q1 and Q2 of 2017 as one
zip, which holds exactly the three files the pipeline needs:
  Divvy_Trips_2017_Q1.csv  Divvy_Trips_2017_Q2.csv  Divvy_Stations_2017_Q1Q2.csv

If all three are already in data/raw/ (for example, downloaded by hand), this
does nothing. Licence: https://divvybikes.com/data-license-agreement
"""
import os
import shutil
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

URL = "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2017_Q1Q2.zip"
RAW = Path(os.environ.get("DATA_DIR", "/app/data")) / "raw"
# file -> the start of its header line, to check the right file was extracted
FILES = {
    "Divvy_Trips_2017_Q1.csv": '"trip_id","start_time"',
    "Divvy_Trips_2017_Q2.csv": '"trip_id","start_time"',
    "Divvy_Stations_2017_Q1Q2.csv": '"id","name"',
}


def have_all() -> bool:
    return all((RAW / f).is_file() and (RAW / f).stat().st_size > 0 for f in FILES)


def download(dest: Path, attempts: int = 3) -> None:
    for attempt in range(1, attempts + 1):
        try:
            print(f"[get_data] Downloading {URL} (attempt {attempt}) ...")
            with urllib.request.urlopen(URL, timeout=60) as r, dest.open("wb") as f:
                total = int(r.headers.get("Content-Length", 0))
                done = 0
                while chunk := r.read(1 << 20):
                    f.write(chunk)
                    done += len(chunk)
                    if total and done % (10 << 20) < (1 << 20):
                        print(f"[get_data]   {done / 1e6:5.1f} of {total / 1e6:.1f} MB")
            return
        except OSError as e:
            print(f"[get_data] Download failed: {e}")
            if attempt == attempts:
                raise
            time.sleep(5 * attempt)


def main():
    if have_all():
        print(f"[get_data] The three CSVs are already in data/raw/, nothing to download.")
        return

    RAW.mkdir(parents=True, exist_ok=True)
    zip_path = RAW / ".download.zip"
    download(zip_path)

    with zipfile.ZipFile(zip_path) as z:
        # Match by file name, ignoring any folder in the zip and macOS "__MACOSX" copies.
        members = {Path(m).name: m for m in z.namelist()
                   if Path(m).name in FILES and "__MACOSX" not in m}
        missing = set(FILES) - set(members)
        if missing:
            sys.exit(f"[get_data] The zip doesn't contain {', '.join(sorted(missing))}.")
        for name, member in members.items():
            with z.open(member) as src, (RAW / name).open("wb") as dst:
                shutil.copyfileobj(src, dst)
    zip_path.unlink()

    for name, header in FILES.items():
        with (RAW / name).open(encoding="utf-8-sig") as f:
            first = f.readline()
        if not first.startswith(header):
            sys.exit(f"[get_data] {name} doesn't look right: its first line is {first[:60]!r}")
        print(f"[get_data] {name}: {(RAW / name).stat().st_size / 1e6:.1f} MB")
    print("[get_data] Done.")


if __name__ == "__main__":
    main()
