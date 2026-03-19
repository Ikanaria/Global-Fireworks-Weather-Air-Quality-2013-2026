"""
async_reference.py
============================
Collects weather, air quality (Open-Meteo) and soil data (NASA POWER)
for reference days - same season as fireworks events but no fireworks.
Strategy: 7 days before AND 7 days after each event (same weekday, same season).
This allows direct comparison: event day vs. comparable non-event day.
Run date_calc.py first to generate event_dates.json.

Performance: Weather + Air quality are fetched in parallel via ThreadPoolExecutor.
NASA POWER (soil) remains sequential due to stricter rate limits.
Estimated runtime: ~4.4h (vs ~6.6h fully sequential).
No additional packages needed - ThreadPoolExecutor is part of Python stdlib.
"""

import requests
import json
import time
import logging
import csv
from concurrent.futures import ThreadPoolExecutor
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

DATA_DIR = Path("raw_data_reference")
DATA_DIR.mkdir(exist_ok=True)

CHECKPOINT_FILE = "checkpoint_reference.json"

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
    "new_years_eve":      {22, 23, 0, 1, 2},
    "independence_day":   {20, 21, 22, 23},
    "canada_day":         {21, 22, 23,  0},
    "bastille_day":       {21, 22, 23,  0},
    "bonfire_night":      {18, 19, 20, 21, 22},
    "australia_day":      {20, 21, 22, 23},
    "singapore_natday":   {19, 20, 21, 22},
    "brazil_indep":       {20, 21, 22, 23},
    "las_fallas":         {12, 13, 14, 15, 21, 22, 23},
    "el_salvador_fire":   {17, 18, 19, 20, 21},
    "rhein_flammen_1":    {21, 22, 23},
    "rhein_flammen_2":    {21, 22, 23},
    "rhein_flammen_3":    {21, 22, 23},
    "chinese_new_year":   {22, 23, 0, 1, 2},
    "diwali":             {17, 18, 19, 20, 21, 22, 23},
    "loy_krathong":       {17, 18, 19, 20, 21, 22, 23},
    "eid_al_adha":        {16, 17, 18, 19, 20, 21},
    "nagaoka":            {18, 19, 20, 21, 22},
    "katakai":            {18, 19, 20, 21, 22},
    "malta_fireworks":    {20, 21, 22, 23},
}

OFFSETS = {
    "minus_7": -7,
    "plus_7":  +7,
}
OFFSET_ORDER = ["minus_7", "plus_7"]

ALL_EVENTS = {
    "new_years_eve": {
        "date_key": "new_years_eve",
        "locations": [
            {"country": "Germany",              "city": "Berlin",         "coordinates": (52.5200,   13.4050)},
            {"country": "France",               "city": "Paris",          "coordinates": (48.8566,    2.3522)},
            {"country": "United Kingdom",       "city": "London",         "coordinates": (51.5074,   -0.1278)},
            {"country": "Australia",            "city": "Sydney",         "coordinates": (-33.8688, 151.2093)},
            {"country": "Brazil",               "city": "Rio de Janeiro", "coordinates": (-22.9068, -43.1729)},
            {"country": "Russian Federation",   "city": "Moscow",         "coordinates": (55.7558,   37.6173)},
            {"country": "Japan",                "city": "Tokyo",          "coordinates": (35.6895,  139.6917)},
            {"country": "United States",        "city": "New York",       "coordinates": (40.7128,  -74.0060)},
            {"country": "United Arab Emirates", "city": "Dubai",          "coordinates": (25.2048,   55.2708)},
            {"country": "China",                "city": "Beijing",        "coordinates": (39.9042,  116.4074)},
        ]
    },
    "independence_day": {
        "date_key": "independence_day",
        "locations": [
            {"country": "United States", "city": "Washington DC", "coordinates": (38.8951,  -77.0364)},
            {"country": "United States", "city": "New York",      "coordinates": (40.7128,  -74.0060)},
            {"country": "United States", "city": "Los Angeles",   "coordinates": (34.0522, -118.2437)},
            {"country": "United States", "city": "Chicago",       "coordinates": (41.8781,  -87.6298)},
        ]
    },
    "canada_day": {
        "date_key": "canada_day",
        "locations": [
            {"country": "Canada", "city": "Ottawa",    "coordinates": (45.4215,  -75.6972)},
            {"country": "Canada", "city": "Toronto",   "coordinates": (43.6532,  -79.3832)},
            {"country": "Canada", "city": "Vancouver", "coordinates": (49.2827, -123.1207)},
        ]
    },
    "bastille_day": {
        "date_key": "bastille_day",
        "locations": [
            {"country": "France", "city": "Paris",     "coordinates": (48.8566, 2.3522)},
            {"country": "France", "city": "Lyon",      "coordinates": (45.7640, 4.8357)},
            {"country": "France", "city": "Marseille", "coordinates": (43.2965, 5.3698)},
        ]
    },
    "bonfire_night": {
        "date_key": "bonfire_night",
        "locations": [
            {"country": "United Kingdom", "city": "London",     "coordinates": (51.5074, -0.1278)},
            {"country": "United Kingdom", "city": "Birmingham", "coordinates": (52.4862, -1.8904)},
            {"country": "United Kingdom", "city": "Manchester", "coordinates": (53.4808, -2.2426)},
            {"country": "Ireland",        "city": "Dublin",     "coordinates": (53.3331, -6.2489)},
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
            {"country": "Brazil", "city": "Brasilia",       "coordinates": (-15.7939, -47.8828)},
            {"country": "Brazil", "city": "Rio de Janeiro", "coordinates": (-22.9068, -43.1729)},
            {"country": "Brazil", "city": "Sao Paulo",      "coordinates": (-23.5505, -46.6333)},
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
            {"country": "Germany", "city": "Bonn",    "coordinates": (50.7374, 7.0982)},
            {"country": "Germany", "city": "Cologne", "coordinates": (50.9333, 6.9500)},
        ]
    },
    "rhein_flammen_2": {
        "date_key": "rhein_flammen_2",
        "locations": [
            {"country": "Germany", "city": "St. Goar", "coordinates": (50.1533, 7.7144)},
            {"country": "Germany", "city": "Bingen",   "coordinates": (49.9667, 7.8989)},
        ]
    },
    "rhein_flammen_3": {
        "date_key": "rhein_flammen_3",
        "locations": [
            {"country": "Germany", "city": "Koblenz", "coordinates": (50.3569, 7.5890)},
        ]
    },
    "chinese_new_year": {
        "date_key": "chinese_new_year",
        "locations": [
            {"country": "China",             "city": "Beijing",       "coordinates": (39.9042,  116.4074)},
            {"country": "China",             "city": "Shanghai",      "coordinates": (31.2304,  121.4737)},
            {"country": "China",             "city": "Hong Kong",     "coordinates": (22.3193,  114.1694)},
            {"country": "China",             "city": "Guangzhou",     "coordinates": (23.1291,  113.2644)},
            {"country": "Singapore",         "city": "Singapore",     "coordinates": ( 1.3521,  103.8198)},
            {"country": "Malaysia",          "city": "Kuala Lumpur",  "coordinates": ( 3.1390,  101.6869)},
            {"country": "Viet Nam",          "city": "Hanoi",         "coordinates": (21.0285,  105.8542)},
            {"country": "Republic of Korea", "city": "Seoul",         "coordinates": (37.5665,  126.9780)},
            {"country": "United States",     "city": "San Francisco", "coordinates": (37.7749, -122.4194)},
        ]
    },
    "diwali": {
        "date_key": "diwali",
        "locations": [
            {"country": "India",     "city": "New Delhi",   "coordinates": (28.6139,  77.2090)},
            {"country": "India",     "city": "Mumbai",      "coordinates": (19.0760,  72.8777)},
            {"country": "India",     "city": "Jaipur",      "coordinates": (26.9124,  75.7873)},
            {"country": "India",     "city": "Varanasi",    "coordinates": (25.3176,  82.9739)},
            {"country": "India",     "city": "Amritsar",    "coordinates": (31.6340,  74.8723)},
            {"country": "Nepal",     "city": "Kathmandu",   "coordinates": (27.7172,  85.3240)},
            {"country": "Singapore", "city": "Singapore",   "coordinates": ( 1.3521, 103.8198)},
            {"country": "Malaysia",  "city": "Kuala Lumpur","coordinates": ( 3.1390, 101.6869)},
        ]
    },
    "loy_krathong": {
        "date_key": "loy_krathong",
        "locations": [
            {"country": "Thailand", "city": "Bangkok",    "coordinates": (13.7563, 100.5018)},
            {"country": "Thailand", "city": "Chiang Mai", "coordinates": (18.7883,  98.9853)},
            {"country": "Thailand", "city": "Sukhothai",  "coordinates": (17.0054,  99.8258)},
            {"country": "Thailand", "city": "Phuket",     "coordinates": ( 7.8804,  98.3923)},
            {"country": "Myanmar",  "city": "Yangon",     "coordinates": (16.8661,  96.1951)},
            {"country": "Lao PDR",  "city": "Vientiane",  "coordinates": (17.9757, 102.6331)},
        ]
    },
    "eid_al_adha": {
        "date_key": "eid_al_adha",
        "locations": [
            {"country": "Saudi Arabia",        "city": "Riyadh",      "coordinates": (24.7136,  46.6753)},
            {"country": "Saudi Arabia",        "city": "Mecca",       "coordinates": (21.3891,  39.8579)},
            {"country": "United Arab Emirates","city": "Dubai",       "coordinates": (25.2048,  55.2708)},
            {"country": "Egypt",               "city": "Cairo",       "coordinates": (30.0444,  31.2357)},
            {"country": "Türkiye",             "city": "Istanbul",    "coordinates": (41.0082,  28.9784)},
            {"country": "Morocco",             "city": "Casablanca",  "coordinates": (33.5731,  -7.5898)},
            {"country": "Pakistan",            "city": "Karachi",     "coordinates": (24.8607,  67.0011)},
            {"country": "Indonesia",           "city": "Jakarta",     "coordinates": (-6.2088, 106.8456)},
            {"country": "Malaysia",            "city": "Kuala Lumpur","coordinates": ( 3.1390, 101.6869)},
        ]
    },
    "nagaoka": {
        "date_key": "nagaoka",
        "locations": [
            {"country": "Japan", "city": "Nagaoka", "coordinates": (37.4469, 138.8509)},
            {"country": "Japan", "city": "Tokyo",   "coordinates": (35.6895, 139.6917)},
        ]
    },
    "katakai": {
        "date_key": "katakai",
        "locations": [
            {"country": "Japan", "city": "Katakai", "coordinates": (37.5000, 138.9833)},
            {"country": "Japan", "city": "Niigata", "coordinates": (37.9162, 139.0364)},
        ]
    },
    "malta_fireworks": {
        "date_key": "malta_fireworks",
        "locations": [
            {"country": "Malta", "city": "Valletta",   "coordinates": (35.8997, 14.5146)},
            {"country": "Malta", "city": "Marsaxlokk", "coordinates": (35.8417, 14.5431)},
        ]
    },
}

# ---------------- CHECKPOINT ----------------

def load_checkpoint():
    if Path(CHECKPOINT_FILE).exists():
        return json.load(open(CHECKPOINT_FILE))
    return {"event": None, "city": None, "year": None, "offset": None}

def save_checkpoint(event, city, year, offset):
    json.dump({"event": event, "city": city, "year": year, "offset": offset},
              open(CHECKPOINT_FILE, "w"))

# ---------------- API HELPERS ----------------

def fetch_json(url):
    """Fetch JSON with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logging.warning(f"Retry {attempt+1}/{MAX_RETRIES}: {e}")
            time.sleep(10)
    raise RuntimeError(f"API request failed: {url}")


def fetch_weather_and_air_parallel(lat, lon, start_date, end_date):
    """
    Fetch weather and air quality simultaneously using two threads.
    Both endpoints are Open-Meteo (different hosts) so parallel calls are safe.
    Returns (weather_json, air_json).
    """
    weather_url = (
        f"{WEATHER_URL}?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly={weather_params}&timezone=auto"
    )
    air_url = (
        f"{AIR_URL}?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly={air_params}&timezone=auto"
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        f_weather = executor.submit(fetch_json, weather_url)
        f_air     = executor.submit(fetch_json, air_url)
        weather   = f_weather.result()
        air       = f_air.result()
    return weather, air


def fetch_soil(lat, lon, start_date, end_date):
    """Fetch soil data from NASA POWER (sequential - stricter rate limits)."""
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

def write_rows(event_name, ref_type, location, year,
               ref_date, days_offset, weather, air, soil):
    """Write filtered reference hours to CSV."""
    csv_path = DATA_DIR / f"{event_name}_reference.csv"
    file_exists = csv_path.exists()

    lat, lon = location["coordinates"]
    valid_hours = REFERENCE_HOURS[event_name]

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

            soil_key = dt.strftime("%Y%m%d%H")
            s = soil_data.get(soil_key, {})

            row = {
                "event":                  event_name,
                "reference_type":         ref_type,
                "country":                location["country"],
                "city":                   location["city"],
                "latitude":               lat,
                "longitude":              lon,
                "time":                   ts,
                "year":                   year,
                "days_offset_from_event": days_offset,
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

with open("event_dates.json") as f:
    EVENT_DATES = json.load(f)

checkpoint    = load_checkpoint()
resume_event  = checkpoint["event"]
resume_city   = checkpoint["city"]
resume_year   = checkpoint["year"]
resume_offset = checkpoint["offset"]
resume_mode   = resume_event is not None

for event_name, event_config in ALL_EVENTS.items():

    # Skip events before checkpoint
    if resume_mode and event_name != resume_event:
        continue
    if resume_mode and event_name == resume_event:
        resume_mode = False  # unlock from here, finer checks below

    date_key = event_config["date_key"]
    logging.info(f"Reference days for: {event_name}")

    for location in event_config["locations"]:
        city = location["city"]
        lat, lon = location["coordinates"]

        # Skip cities before checkpoint city
        if resume_city and city != resume_city:
            continue
        if resume_city and city == resume_city:
            resume_city = None  # unlock city-level resume

        for year in YEARS:

            # Skip years before checkpoint year
            if resume_year and year < resume_year:
                continue

            event_date_str = EVENT_DATES[str(year)][date_key]
            event_date     = date.fromisoformat(event_date_str)

            for ref_type, offset_days in OFFSETS.items():

                # Skip offsets before checkpoint within resume year
                if resume_year and year == resume_year and resume_offset is not None:
                    chk_idx = OFFSET_ORDER.index(
                        "minus_7" if resume_offset == -7 else "plus_7")
                    if OFFSET_ORDER.index(ref_type) < chk_idx:
                        continue

                ref_date   = event_date + timedelta(days=offset_days)
                start_date = ref_date.strftime("%Y-%m-%d")
                end_date   = ref_date.strftime("%Y-%m-%d")

                logging.info(f"  {city} {year} {ref_type} ({ref_date})")

                try:
                    # ── Parallel: Weather + Air ──────────────────────
                    weather, air = fetch_weather_and_air_parallel(
                        lat, lon, start_date, end_date)
                    time.sleep(REQUEST_DELAY)

                    # ── Sequential: Soil ─────────────────────────────
                    soil = fetch_soil(lat, lon, start_date, end_date)

                    write_rows(event_name, ref_type, location, year,
                               ref_date, offset_days, weather, air, soil)

                    save_checkpoint(event_name, city, year, offset_days)
                    resume_year   = None  # clear after first successful write
                    resume_offset = None
                    time.sleep(REQUEST_DELAY)

                except Exception as e:
                    logging.error(
                        f"{event_name} {city} {year} {ref_type} failed: {e}")

        time.sleep(2)

logging.info("All reference day data collected.")