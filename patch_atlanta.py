"""
patch_atlanta.py
================
Patches missing Atlanta (United States) data in megacity.csv.

Missing tasks identified from log (429 rate limit at midnight):
  2021: ref_minus7, ref_plus7
  2022: event, ref_minus14, ref_minus7, ref_plus7
  2023: event, ref_minus14, ref_minus7, ref_plus7
  2024: event, ref_minus14, ref_minus7, ref_plus7

Uses same logic, field names and CSV structure as async_extended_cities_collector.py.
Checks for existing rows before appending to avoid duplicates.
"""

import requests
import csv
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ── CONFIG (identical with extended_cities_collector.py) ──────────────────────

WEATHER_URL = "https://archive-api.open-meteo.com/v1/era5"
AIR_URL     = "https://air-quality-api.open-meteo.com/v1/air-quality"
SOIL_URL    = "https://power.larc.nasa.gov/api/temporal/hourly/point"

REQUEST_DELAY = 4
MAX_RETRIES   = 3

DATA_DIR      = Path("raw_data_extended_cities")
CSV_PATH      = DATA_DIR / "megacity.csv"

SILVESTER_HOURS = {22, 23, 0, 1, 2}

weather_params = (
    "temperature_2m,relative_humidity_2m,dew_point_2m,"
    "apparent_temperature,precipitation,rain,snowfall,"
    "snow_depth,weathercode,surface_pressure,"
    "cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,"
    "shortwave_radiation,direct_radiation,diffuse_radiation,"
    "wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
    "boundary_layer_height"
)
air_params  = (
    "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,"
    "sulphur_dioxide,ozone,aerosol_optical_depth,"
    "dust,uv_index"
)
soil_params = "GWETTOP,GWETROOT,GWETPROF,TSOIL1"

WEATHER_COLS = [c.strip() for c in weather_params.split(",")]
AIR_COLS     = [c.strip() for c in air_params.split(",")]

FIELDNAMES = (
    ["category", "country", "city", "latitude", "longitude",
     "time", "year", "data_type"]
    + [f"w_{c}" for c in WEATHER_COLS]
    + [f"a_{c}" for c in AIR_COLS]
    + ["s_soil_moisture_top", "s_soil_moisture_root",
       "s_soil_moisture_profile", "s_soil_temperature_1"]
)

# ── ATLANTA ───────────────────────────────────────────────────────────────────

CITY = {
    "category": "megacity",
    "country":  "United States",
    "city":     "Atlanta",
    "lat":      33.7490,
    "lon":     -84.3880,
}

# Missing Tasks from Log
MISSING_TASKS = [
    (2021, "ref_minus7"),
    (2021, "ref_plus7"),
    (2022, "event"),
    (2022, "ref_minus14"),
    (2022, "ref_minus7"),
    (2022, "ref_plus7"),
    (2023, "event"),
    (2023, "ref_minus14"),
    (2023, "ref_minus7"),
    (2023, "ref_plus7"),
    (2024, "event"),
    (2024, "ref_minus14"),
    (2024, "ref_minus7"),
    (2024, "ref_plus7"),
    (2025, "event"),
    (2025, "ref_minus14"),
    (2025, "ref_minus7"),
    (2025, "ref_plus7"),
]

# ── DUPLICATE CHECK ───────────────────────────────────────────────────────────

def load_existing_keys():
    """Loads all (city, year, data_type) combinations from CSV."""
    keys = set()
    if not CSV_PATH.exists():
        return keys
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["city"] == "Atlanta":
                keys.add((row["city"], int(row["year"]), row["data_type"]))
    logging.info(f"Bestehende Atlanta-Einträge in CSV: {len(keys)}")
    return keys

# ── API HELPERS ───────────────────────────────────────────────────────────────

def fetch_json(url):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logging.warning(f"Retry {attempt+1}/{MAX_RETRIES}: {e}")
            time.sleep(10)
    raise RuntimeError(f"API failed after {MAX_RETRIES} retries: {url}")


def fetch_weather_and_air_parallel(lat, lon, start_str, end_str):
    weather_url = (
        f"{WEATHER_URL}?latitude={lat}&longitude={lon}"
        f"&start_date={start_str}&end_date={end_str}"
        f"&hourly={weather_params}&timezone=auto"
    )
    air_url = (
        f"{AIR_URL}?latitude={lat}&longitude={lon}"
        f"&start_date={start_str}&end_date={end_str}"
        f"&hourly={air_params}&timezone=auto"
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        f_weather = executor.submit(fetch_json, weather_url)
        f_air     = executor.submit(fetch_json, air_url)
        weather   = f_weather.result()
        air       = f_air.result()
    return weather, air


def fetch_soil(lat, lon, start_date, end_date):
    start = start_date.replace("-", "")
    end   = end_date.replace("-", "")
    url = (
        f"{SOIL_URL}?parameters={soil_params}&community=AG"
        f"&longitude={lon}&latitude={lat}"
        f"&start={start}&end={end}&format=JSON&time-standard=LST"
    )
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logging.warning(f"Soil retry {attempt+1}/{MAX_RETRIES}: {e}")
            time.sleep(10)
    raise RuntimeError("NASA POWER failed after max retries")

# ── WRITE ─────────────────────────────────────────────────────────────────────

def write_rows(city, data_type, weather, air, soil):
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
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)

        for idx, ts in enumerate(weather["hourly"]["time"]):
            dt = datetime.fromisoformat(ts)
            if dt.hour not in SILVESTER_HOURS:
                continue

            soil_key = dt.strftime("%Y%m%d%H")
            s = soil_data.get(soil_key, {})

            row = {
                "category":  city["category"],
                "country":   city["country"],
                "city":      city["city"],
                "latitude":  city["lat"],
                "longitude": city["lon"],
                "time":      ts,
                "year":      dt.year,
                "data_type": data_type,
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

# ── MAIN ──────────────────────────────────────────────────────────────────────

existing_keys = load_existing_keys()
lat = CITY["lat"]
lon = CITY["lon"]

skipped  = 0
patched  = 0
failed   = 0

logging.info(f"Starte Atlanta-Patch: {len(MISSING_TASKS)} Tasks")

for year, data_type in MISSING_TASKS:

    key = ("Atlanta", year, data_type)

    if key in existing_keys:
        logging.info(f"  SKIP {year} {data_type} – bereits vorhanden")
        skipped += 1
        continue

    event_date = date(year, 12, 31)
    offsets = {
        "event":       (event_date - timedelta(days=1),  event_date + timedelta(days=1)),
        "ref_minus14": (event_date - timedelta(days=15), event_date - timedelta(days=13)),
        "ref_minus7":  (event_date - timedelta(days=8),  event_date - timedelta(days=6)),
        "ref_plus7":   (event_date + timedelta(days=6),  event_date + timedelta(days=8)),
    }
    start_d, end_d = offsets[data_type]
    start_str = start_d.strftime("%Y-%m-%d")
    end_str   = end_d.strftime("%Y-%m-%d")

    logging.info(f"  PATCH {year} {data_type} ({start_str} → {end_str})")

    try:
        weather, air = fetch_weather_and_air_parallel(lat, lon, start_str, end_str)
        time.sleep(REQUEST_DELAY)

        soil = fetch_soil(lat, lon, start_str, end_str)
        time.sleep(REQUEST_DELAY)

        n = write_rows(CITY, data_type, weather, air, soil)
        logging.info(f"  ✓ {year} {data_type}: {n} Zeilen geschrieben")
        patched += 1

    except Exception as e:
        logging.error(f"  ✗ {year} {data_type} failed: {e}")
        failed += 1

logging.info(
    f"\nPatch abgeschlossen: {patched} gepatcht, {skipped} übersprungen, {failed} fehlgeschlagen"
)
