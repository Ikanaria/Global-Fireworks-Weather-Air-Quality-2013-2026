"""
variable_events_collector.py
=============================
Collects weather, air quality (Open-Meteo) and soil data (NASA POWER)
for all variable-date (lunar calendar) fireworks events 2013-2025.
Run date_calc.py first to generate event_dates.json.
"""

import requests
import json
import time
import logging
import csv
from datetime import datetime, date, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------- CONFIG ----------------

YEARS = range(2013, 2026)

WEATHER_URL = "https://archive-api.open-meteo.com/v1/era5"
AIR_URL     = "https://air-quality-api.open-meteo.com/v1/air-quality"
SOIL_URL    = "https://power.larc.nasa.gov/api/temporal/hourly/point"

REQUEST_DELAY = 4
MAX_RETRIES   = 3

DATA_DIR = Path("raw_data_variable_events")
DATA_DIR.mkdir(exist_ok=True)

CHECKPOINT_FILE = "checkpoint_variable.json"

# Event-specific valid hours based on when fireworks actually occur
EVENT_HOURS = {
    "chinese_new_year": {22, 23, 0, 1, 2},      # midnight focus
    "diwali":           {17, 18, 19, 20, 21, 22, 23},
    "loy_krathong":     {17, 18, 19, 20, 21, 22, 23},
    "eid_al_adha":      {16, 17, 18, 19, 20, 21},
    "nagaoka":          {18, 19, 20, 21, 22},
    "katakai":          {18, 19, 20, 21, 22},
    "malta_fireworks":  {20, 21, 22, 23},
}

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
    ["event", "country", "city", "latitude", "longitude",
     "time", "year", "is_event_day"]
    + [f"w_{c}" for c in WEATHER_COLS]
    + [f"a_{c}" for c in AIR_COLS]
    + ["s_soil_moisture_top", "s_soil_moisture_root",
       "s_soil_moisture_profile", "s_soil_temperature_1"]
)

# Variable events with all relevant locations
# Multiple cities per event where fireworks are culturally significant
VARIABLE_EVENTS = {
    "chinese_new_year": {
        "date_key": "chinese_new_year",
        "locations": [
            {"country": "China",          "city": "Beijing",        "coordinates": (39.9042,  116.4074)},
            {"country": "China",          "city": "Shanghai",       "coordinates": (31.2304,  121.4737)},
            {"country": "China",          "city": "Hong Kong",      "coordinates": (22.3193,  114.1694)},
            {"country": "China",          "city": "Guangzhou",      "coordinates": (23.1291,  113.2644)},
            {"country": "Singapore",      "city": "Singapore",      "coordinates": ( 1.3521,  103.8198)},
            {"country": "Malaysia",       "city": "Kuala Lumpur",   "coordinates": ( 3.1390,  101.6869)},
            {"country": "Viet Nam",       "city": "Hanoi",          "coordinates": (21.0285,  105.8542)},
            {"country": "Republic of Korea", "city": "Seoul",       "coordinates": (37.5665,  126.9780)},
            {"country": "United States",  "city": "San Francisco",  "coordinates": (37.7749, -122.4194)},
        ]
    },
    "diwali": {
        "date_key": "diwali",
        "locations": [
            {"country": "India",          "city": "New Delhi",      "coordinates": (28.6139,   77.2090)},
            {"country": "India",          "city": "Mumbai",         "coordinates": (19.0760,   72.8777)},
            {"country": "India",          "city": "Jaipur",         "coordinates": (26.9124,   75.7873)},
            {"country": "India",          "city": "Varanasi",       "coordinates": (25.3176,   82.9739)},
            {"country": "India",          "city": "Amritsar",       "coordinates": (31.6340,   74.8723)},
            {"country": "Nepal",          "city": "Kathmandu",      "coordinates": (27.7172,   85.3240)},
            {"country": "Sri Lanka",      "city": "Colombo",        "coordinates": ( 6.9271,   79.8612)},
            {"country": "Singapore",      "city": "Singapore",      "coordinates": ( 1.3521,  103.8198)},
            {"country": "Malaysia",       "city": "Kuala Lumpur",   "coordinates": ( 3.1390,  101.6869)},
        ]
    },
    "loy_krathong": {
        "date_key": "loy_krathong",
        "locations": [
            {"country": "Thailand",       "city": "Bangkok",        "coordinates": (13.7563,  100.5018)},
            {"country": "Thailand",       "city": "Chiang Mai",     "coordinates": (18.7883,   98.9853)},
            {"country": "Thailand",       "city": "Sukhothai",      "coordinates": (17.0054,   99.8258)},
            {"country": "Thailand",       "city": "Phuket",         "coordinates": ( 7.8804,   98.3923)},
            {"country": "Myanmar",        "city": "Yangon",         "coordinates": (16.8661,   96.1951)},  # Tazaungdaing
            {"country": "Lao PDR",        "city": "Vientiane",      "coordinates": (17.9757,  102.6331)},  # Boun That
        ]
    },
    "eid_al_adha": {
        "date_key": "eid_al_adha",
        "locations": [
            {"country": "Saudi Arabia",   "city": "Riyadh",         "coordinates": (24.7136,   46.6753)},
            {"country": "Saudi Arabia",   "city": "Mecca",          "coordinates": (21.3891,   39.8579)},
            {"country": "United Arab Emirates", "city": "Dubai",    "coordinates": (25.2048,   55.2708)},
            {"country": "United Arab Emirates", "city": "Abu Dhabi","coordinates": (24.4539,   54.3773)},
            {"country": "Egypt",          "city": "Cairo",          "coordinates": (30.0444,   31.2357)},
            {"country": "Türkiye",         "city": "Istanbul",       "coordinates": (41.0082,   28.9784)},
            {"country": "Morocco",        "city": "Casablanca",     "coordinates": (33.5731,   -7.5898)},
            {"country": "Pakistan",       "city": "Karachi",        "coordinates": (24.8607,   67.0011)},
            {"country": "Indonesia",      "city": "Jakarta",        "coordinates": (-6.2088,  106.8456)},
            {"country": "Malaysia",       "city": "Kuala Lumpur",   "coordinates": ( 3.1390,  101.6869)},
        ]
    },
    "nagaoka": {
        "date_key": "nagaoka",
        "locations": [
            {"country": "Japan", "city": "Nagaoka",  "coordinates": (37.4469, 138.8509)},
            {"country": "Japan", "city": "Tokyo",    "coordinates": (35.6895, 139.6917)},  # reference city
        ]
    },
    "katakai": {
        "date_key": "katakai",
        "locations": [
            {"country": "Japan", "city": "Katakai",  "coordinates": (37.5000, 138.9833)},
            {"country": "Japan", "city": "Niigata",  "coordinates": (37.9162, 139.0364)},
        ]
    },
    "malta_fireworks": {
        "date_key": "malta_fireworks",
        "locations": [
            {"country": "Malta", "city": "Valletta",     "coordinates": (35.8997,  14.5146)},
            {"country": "Malta", "city": "Marsaxlokk",   "coordinates": (35.8417,  14.5431)},
        ]
    },
}

# ---------------- CHECKPOINT ----------------

def load_checkpoint():
    if Path(CHECKPOINT_FILE).exists():
        return json.load(open(CHECKPOINT_FILE))
    return {"event": None, "location": None, "year": None}

def save_checkpoint(event, location, year):
    json.dump({"event": event, "location": location, "year": year},
              open(CHECKPOINT_FILE, "w"))

# ---------------- API HELPERS ----------------

def fetch_json(url):
    """Fetch JSON from Open-Meteo APIs with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logging.warning(f"Retry {attempt+1}/{MAX_RETRIES}: {e}")
            time.sleep(10)
    raise RuntimeError(f"API request failed: {url}")

def fetch_soil(lat, lon, start_date, end_date):
    """Fetch soil data from NASA POWER API."""
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
    raise RuntimeError("NASA POWER request failed")

# ---------------- WRITE ----------------

def write_rows(event_name, location, year, event_date, weather, air, soil):
    """Write filtered hours to event CSV."""
    csv_path = DATA_DIR / f"{event_name}.csv"
    file_exists = csv_path.exists()

    lat, lon = location["coordinates"]
    valid_hours = EVENT_HOURS[event_name]

    # Build soil lookup dict
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

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()

        for idx, ts in enumerate(weather["hourly"]["time"]):
            dt = datetime.fromisoformat(ts)
            if dt.hour not in valid_hours:
                continue

            is_event_day = 1 if dt.date() == event_date else 0
            soil_key = dt.strftime("%Y%m%d%H")
            s = soil_data.get(soil_key, {})

            row = {
                "event":     event_name,
                "country":   location["country"],
                "city":      location["city"],
                "latitude":  lat,
                "longitude": lon,
                "time":      ts,
                "year":      year,
                "is_event_day": is_event_day,
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

# ---------------- MAIN ----------------

# Load event dates
with open("event_dates.json") as f:
    EVENT_DATES = json.load(f)

checkpoint = load_checkpoint()
resume = checkpoint["event"] is None

for event_name, event_config in VARIABLE_EVENTS.items():

    if not resume:
        if event_name == checkpoint["event"]:
            resume = True
        else:
            continue

    date_key = event_config["date_key"]
    logging.info(f"Event: {event_name}")

    for location in event_config["locations"]:
        city = location["city"]

        for year in YEARS:
            event_date_str = EVENT_DATES[str(year)][date_key]
            event_date     = date.fromisoformat(event_date_str)

            # Collect day before, event day, and day after for context
            start_date = (event_date - timedelta(days=1)).strftime("%Y-%m-%d")
            end_date   = (event_date + timedelta(days=1)).strftime("%Y-%m-%d")

            lat, lon = location["coordinates"]
            logging.info(f"  {city} {year} ({event_date_str})")

            try:
                # Weather
                weather_url = (
                    f"{WEATHER_URL}?latitude={lat}&longitude={lon}"
                    f"&start_date={start_date}&end_date={end_date}"
                    f"&hourly={weather_params}&timezone=auto"
                )
                weather = fetch_json(weather_url)
                time.sleep(REQUEST_DELAY)

                # Air quality
                air_url = (
                    f"{AIR_URL}?latitude={lat}&longitude={lon}"
                    f"&start_date={start_date}&end_date={end_date}"
                    f"&hourly={air_params}&timezone=auto"
                )
                air = fetch_json(air_url)
                time.sleep(REQUEST_DELAY)

                # Soil
                soil = fetch_soil(lat, lon, start_date, end_date)

                write_rows(event_name, location, year, event_date, weather, air, soil)
                save_checkpoint(event_name, city, year)

                time.sleep(REQUEST_DELAY)

            except Exception as e:
                logging.error(f"{event_name} {city} {year} failed: {e}")

        time.sleep(2)

logging.info("All variable event data collected.")