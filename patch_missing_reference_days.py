"""
patch_missing_reference_days.py
================================
Adds missing entries to the Reference Day CSVs.
Identified from the logs dated 2026-02-22/23.

Missing entries:
  new_years_eve    | Rio de Janeiro | 2016 | minus_7  → 2016-12-24
  canada_day       | Vancouver      | 2018 | minus_14 → 2018-06-17  (Permission denied)
  canada_day       | Vancouver      | 2018 | minus_7  → 2018-06-24  (Permission denied)
  el_salvador_fire | San Salvador   | 2024 | plus_7   → 2024-09-07
  el_salvador_fire | San Salvador   | 2025 | minus_14 → 2025-08-17

"""

import requests
import time
import logging
import csv
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DATA_DIR = Path("raw_data_reference")

WEATHER_URL = "https://archive-api.open-meteo.com/v1/era5"
AIR_URL     = "https://air-quality-api.open-meteo.com/v1/air-quality"
SOIL_URL    = "https://power.larc.nasa.gov/api/temporal/hourly/point"

weather_params = (
    "temperature_2m,relative_humidity_2m,dew_point_2m,"
    "apparent_temperature,precipitation,rain,snowfall,"
    "snow_depth,weathercode,surface_pressure,"
    "cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,"
    "shortwave_radiation,direct_radiation,diffuse_radiation,"
    "wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
    "boundary_layer_height"
)

air_params = (
    "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,"
    "sulphur_dioxide,ozone,aerosol_optical_depth,"
    "dust,uv_index"
)

soil_params = "GWETTOP,GWETROOT,GWETPROF,TSOIL1"

WEATHER_COLS = [c.strip() for c in weather_params.split(",")]
AIR_COLS     = [c.strip() for c in air_params.split(",")]

FIELDNAMES = (
    ["event", "reference_type", "country", "city",
     "latitude", "longitude", "time", "year",
     "days_offset_from_event"]
    + [f"w_{c}" for c in WEATHER_COLS]
    + [f"a_{c}" for c in AIR_COLS]
    + ["s_soil_moisture_top", "s_soil_moisture_root",
       "s_soil_moisture_profile", "s_soil_temperature_1"]
)

REFERENCE_HOURS = {
    "new_years_eve":    {22, 23, 0, 1, 2},
    "canada_day":       {21, 22, 23, 0},
    "el_salvador_fire": {17, 18, 19, 20, 21},
}

MAX_RETRIES = 5
RETRY_DELAY = 45

MISSING_ENTRIES = [
    {
        "event":          "new_years_eve",
        "reference_type": "minus_7",
        "country":        "Brazil",
        "city":           "Rio de Janeiro",
        "lat":            -22.9068,
        "lon":            -43.1729,
        "year":           2016,
        "days_offset":    -7,
        "date":           "2016-12-24",
        "csv_file":       "new_years_eve_reference.csv",
    },
    {
        "event":          "canada_day",
        "reference_type": "minus_14",
        "country":        "Canada",
        "city":           "Vancouver",
        "lat":            49.2827,
        "lon":            -123.1207,
        "year":           2018,
        "days_offset":    -14,
        "date":           "2018-06-17",
        "csv_file":       "canada_day_reference.csv",
    },
    {
        "event":          "canada_day",
        "reference_type": "minus_7",
        "country":        "Canada",
        "city":           "Vancouver",
        "lat":            49.2827,
        "lon":            -123.1207,
        "year":           2018,
        "days_offset":    -7,
        "date":           "2018-06-24",
        "csv_file":       "canada_day_reference.csv",
    },
    {
        "event":          "el_salvador_fire",
        "reference_type": "plus_7",
        "country":        "El Salvador",
        "city":           "San Salvador",
        "lat":            13.6929,
        "lon":            -89.2182,
        "year":           2024,
        "days_offset":    +7,
        "date":           "2024-09-07",
        "csv_file":       "el_salvador_fire_reference.csv",
    },
    {
        "event":          "el_salvador_fire",
        "reference_type": "minus_14",
        "country":        "El Salvador",
        "city":           "San Salvador",
        "lat":            13.6929,
        "lon":            -89.2182,
        "year":           2025,
        "days_offset":    -14,
        "date":           "2025-08-17",
        "csv_file":       "el_salvador_fire_reference.csv",
    },
]


def fetch_json(url: str) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                logging.warning(f"  Retry {attempt+1}/{MAX_RETRIES-1}: {e}")
                time.sleep(RETRY_DELAY)
            else:
                raise RuntimeError(f"API request failed after {MAX_RETRIES} attempts: {url}") from e


def fetch_weather_and_air_parallel(lat, lon, date):
    weather_url = (
        f"{WEATHER_URL}?latitude={lat}&longitude={lon}"
        f"&start_date={date}&end_date={date}"
        f"&hourly={weather_params}&timezone=auto"
    )
    air_url = (
        f"{AIR_URL}?latitude={lat}&longitude={lon}"
        f"&start_date={date}&end_date={date}"
        f"&hourly={air_params}&timezone=auto"
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        f_weather = executor.submit(fetch_json, weather_url)
        f_air     = executor.submit(fetch_json, air_url)
        return f_weather.result(), f_air.result()


def fetch_soil(lat, lon, date):
    date_fmt = date.replace("-", "")
    url = (
        f"{SOIL_URL}?parameters={soil_params}&community=AG"
        f"&longitude={lon}&latitude={lat}"
        f"&start={date_fmt}&end={date_fmt}&format=JSON&time-standard=LST"
    )
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                logging.warning(f"  Soil retry {attempt+1}/{MAX_RETRIES-1}: {e}")
                time.sleep(RETRY_DELAY)
            else:
                raise RuntimeError(f"NASA POWER request failed: {url}") from e


def already_exists(csv_path: Path, city: str, year: int, ref_type: str) -> bool:
    """Checks if at least one row exists for (city, year, reference_type)."""
    if not csv_path.exists():
        return False
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("city") == city
                    and str(row.get("year")) == str(year)
                    and row.get("reference_type") == ref_type):
                return True
    return False


def write_rows(entry: dict, weather: dict, air: dict, soil: dict) -> int:
    """
    Writes hourly rows identically to `write_rows()` in the main script.
    Only hours from `REFERENCE_HOURS[event]` are saved.
    """
    csv_path    = DATA_DIR / entry["csv_file"]
    file_exists = csv_path.exists()
    valid_hours = REFERENCE_HOURS[entry["event"]]

    # Soil indexed by timestamp(Format: YYYYMMDDHH)
    soil_data = {}
    if soil:
        params = soil.get("properties", {}).get("parameter", {})
        for ts_key in params.get("TSOIL1", {}):
            soil_data[ts_key] = {
                "s_soil_moisture_top":     params["GWETTOP"].get(ts_key),
                "s_soil_moisture_root":    params["GWETROOT"].get(ts_key),
                "s_soil_moisture_profile": params["GWETPROF"].get(ts_key),
                "s_soil_temperature_1":    params["TSOIL1"].get(ts_key),
            }

    rows_written = 0
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()

        for idx, ts in enumerate(weather["hourly"]["time"]):
            dt = datetime.fromisoformat(ts)
            if dt.hour not in valid_hours:
                continue

            soil_key = dt.strftime("%Y%m%d%H")
            s = soil_data.get(soil_key, {})

            row = {
                "event":                  entry["event"],
                "reference_type":         entry["reference_type"],
                "country":                entry["country"],
                "city":                   entry["city"],
                "latitude":               entry["lat"],
                "longitude":              entry["lon"],
                "time":                   ts,
                "year":                   entry["year"],
                "days_offset_from_event": entry["days_offset"],
            }

            for col in WEATHER_COLS:
                vals = weather["hourly"].get(col)
                row[f"w_{col}"] = vals[idx] if vals else None

            for col in AIR_COLS:
                vals = air["hourly"].get(col)
                row[f"a_{col}"] = vals[idx] if vals else None

            row["s_soil_moisture_top"]     = s.get("s_soil_moisture_top")
            row["s_soil_moisture_root"]    = s.get("s_soil_moisture_root")
            row["s_soil_moisture_profile"] = s.get("s_soil_moisture_profile")
            row["s_soil_temperature_1"]    = s.get("s_soil_temperature_1")

            writer.writerow(row)
            rows_written += 1

    return rows_written


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    logging.info("=" * 60)
    logging.info("PATCH: Adding missing reference day entries")
    logging.info(f"Entries: {len(MISSING_ENTRIES)}")
    logging.info("=" * 60)

    success_count = 0
    fail_count    = 0

    for entry in MISSING_ENTRIES:
        csv_path = DATA_DIR / entry["csv_file"]

        if already_exists(csv_path, entry["city"], entry["year"], entry["reference_type"]):
            logging.info(
                f"  SKIP (vorhanden): {entry['event']} | "
                f"{entry['city']} {entry['year']} {entry['reference_type']}"
            )
            continue

        logging.info(
            f"  Patch: {entry['event']} | {entry['city']} {entry['year']} "
            f"{entry['reference_type']} ({entry['date']})"
        )

        try:
            weather, air = fetch_weather_and_air_parallel(
                entry["lat"], entry["lon"], entry["date"]
            )
            time.sleep(2)

            soil = fetch_soil(entry["lat"], entry["lon"], entry["date"])

            n = write_rows(entry, weather, air, soil)
            logging.info(f"    ✓ {n} Stunden-Zeilen → {entry['csv_file']}")
            success_count += 1

        except Exception as e:
            logging.error(
                f"    ✗ ERROR: {entry['event']} | {entry['city']} "
                f"{entry['year']} {entry['reference_type']}: {e}"
            )
            fail_count += 1

        time.sleep(4)

    logging.info("=" * 60)
    logging.info(f"completed: {success_count} ✓  |  {fail_count} ✗")
    if fail_count > 0:
        logging.warning("The script is idempotent")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
