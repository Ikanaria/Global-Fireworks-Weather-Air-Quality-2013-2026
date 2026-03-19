"""
fixed_events.py
==========================
Collects weather, air quality (Open-Meteo) and soil data (NASA POWER)
for all fixed-date fireworks events 2013-2025.
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

REQUEST_DELAY = 4   # seconds between API calls
MAX_RETRIES   = 3

DATA_DIR = Path("raw_data_fixed_events")
DATA_DIR.mkdir(exist_ok=True)

CHECKPOINT_FILE = "checkpoint_fixed.json"

# Event-specific valid hours based on when fireworks actually occur
EVENT_HOURS = {
    "independence_day":   {20, 21, 22, 23},
    "canada_day":         {21, 22, 23,  0},
    "bastille_day":       {21, 22, 23,  0},
    "bonfire_night":      {18, 19, 20, 21, 22},
    "australia_day":      {20, 21, 22, 23},
    "singapore_natday":   {19, 20, 21, 22},
    "brazil_indep":       {20, 21, 22, 23},
    "las_fallas":         {12, 13, 14, 15, 21, 22, 23},  # Mascletà 14:00 + evening show
    "el_salvador_fire":   {17, 18, 19, 20, 21},
    "rhein_flammen_1":    {21, 22, 23},
    "rhein_flammen_2":    {21, 22, 23},
    "rhein_flammen_3":    {21, 22, 23},
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

# Fixed events - New Year's Eve removed (covered by main silvester script)
FIXED_EVENTS = {
    "independence_day": {
        "date_key": "independence_day",
        "locations": [
            {"country": "United States", "city": "Washington DC",  "coordinates": (38.8951, -77.0364)},
            {"country": "United States", "city": "New York",       "coordinates": (40.7128, -74.0060)},
            {"country": "United States", "city": "Los Angeles",    "coordinates": (34.0522, -118.2437)},
            {"country": "United States", "city": "Chicago",        "coordinates": (41.8781, -87.6298)},
        ]
    },
    "canada_day": {
        "date_key": "canada_day",
        "locations": [
            {"country": "Canada", "city": "Ottawa",    "coordinates": (45.4215, -75.6972)},
            {"country": "Canada", "city": "Toronto",   "coordinates": (43.6532, -79.3832)},
            {"country": "Canada", "city": "Vancouver", "coordinates": (49.2827, -123.1207)},
        ]
    },
    "bastille_day": {
        "date_key": "bastille_day",
        "locations": [
            {"country": "France", "city": "Paris",     "coordinates": (48.8566,   2.3522)},
            {"country": "France", "city": "Lyon",      "coordinates": (45.7640,   4.8357)},
            {"country": "France", "city": "Marseille", "coordinates": (43.2965,   5.3698)},
        ]
    },
    "bonfire_night": {
        "date_key": "bonfire_night",
        "locations": [
            {"country": "United Kingdom", "city": "London",     "coordinates": (51.5074,  -0.1278)},
            {"country": "United Kingdom", "city": "Birmingham",  "coordinates": (52.4862,  -1.8904)},
            {"country": "United Kingdom", "city": "Manchester",  "coordinates": (53.4808,  -2.2426)},
            {"country": "Ireland",        "city": "Dublin",      "coordinates": (53.3331,  -6.2489)},
        ]
    },
    "australia_day": {
        "date_key": "australia_day",
        "locations": [
            {"country": "Australia", "city": "Sydney",    "coordinates": (-33.8688, 151.2093)},
            {"country": "Australia", "city": "Melbourne", "coordinates": (-37.8136, 144.9631)},
            {"country": "Australia", "city": "Canberra",  "coordinates": (-35.2809, 149.1300)},
        ]
    },
    "singapore_natday": {
        "date_key": "singapore_natday",
        "locations": [
            {"country": "Singapore", "city": "Singapore", "coordinates": (1.3521, 103.8198)},
        ]
    },
    "brazil_indep": {
        "date_key": "brazil_indep",
        "locations": [
            {"country": "Brazil", "city": "Brasilia",        "coordinates": (-15.7939, -47.8828)},
            {"country": "Brazil", "city": "Rio de Janeiro",  "coordinates": (-22.9068, -43.1729)},
            {"country": "Brazil", "city": "Sao Paulo",       "coordinates": (-23.5505, -46.6333)},
        ]
    },
    "las_fallas": {
        "date_key": "las_fallas",
        "locations": [
            {"country": "Spain", "city": "Valencia", "coordinates": (39.4699, -0.3763)},
        ]
    },
    "el_salvador_fire": {
        "date_key": "el_salvador_fire",
        "locations": [
            {"country": "El Salvador", "city": "San Salvador", "coordinates": (13.6929, -89.2182)},
        ]
    },
    "rhein_flammen_1": {
        "date_key": "rhein_flammen_1",
        "locations": [
            {"country": "Germany", "city": "Bonn",    "coordinates": (50.7374,  7.0982)},
            {"country": "Germany", "city": "Cologne", "coordinates": (50.9333,  6.9500)},
        ]
    },
    "rhein_flammen_2": {
        "date_key": "rhein_flammen_2",
        "locations": [
            {"country": "Germany", "city": "St. Goar",  "coordinates": (50.1533,  7.7144)},
            {"country": "Germany", "city": "Bingen",    "coordinates": (49.9667,  7.8989)},
        ]
    },
    "rhein_flammen_3": {
        "date_key": "rhein_flammen_3",
        "locations": [
            {"country": "Germany", "city": "Koblenz",  "coordinates": (50.3569,  7.5890)},
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

# ---------------- CSV HELPER ----------------

def get_csv_path(event):
    return DATA_DIR / f"{event}.csv"

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
    raise RuntimeError(f"NASA POWER request failed")

# ---------------- WRITE ----------------

def write_rows(event_name, location, year, event_date, weather, air, soil):
    """Write filtered hours to event CSV."""
    csv_path = get_csv_path(event_name)
    file_exists = csv_path.exists()

    lat, lon = location["coordinates"]
    valid_hours = EVENT_HOURS[event_name]

    # Build soil lookup dict keyed by YYYYMMDDHH
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

            # Mark whether this hour falls on the actual event day
            is_event_day = 1 if dt.date() == event_date else 0

            # NASA POWER timestamp key format
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

for event_name, event_config in FIXED_EVENTS.items():

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

logging.info("All fixed event data collected.")