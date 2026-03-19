"""
async_extended_cities_collector.py
=============================
Collects weather, air quality (Open-Meteo) and soil data (NASA POWER)
for additional cities not covered by the main silvester script.

Three categories of additional locations:
1. MEGACITIES - top global cities by population missing from existing scripts
2. LARGE COUNTRIES - major secondary cities for countries with huge territory
3. EVENT SUPPLEMENTS - culturally important cities for specific fireworks events

Run date_calc.py first to generate event_dates.json.
This script collects Silvester data (Dec 31 22:00 - Jan 01 02:00) for all cities,
plus reference days (±7 days).

Deduplication rule: each city appears only once under its most fitting category.
Megacity > large_country > event_*
Removed duplicates (20 entries): Kolkata(large_country+event_diwali), Dhaka(event_eid),
Chengdu(event_cny), Osaka(event_japan_fw), Manila(event_silvester+event_cny),
Johannesburg(event_silvester), Cape Town(event_silvester), Khartoum(event_eid),
Algiers(event_eid), Bogota(event_silvester), Philadelphia(event_july4),
Boston(event_july4), Madrid(event_silvester), Amsterdam(event_silvester),
Zurich(event_silvester), Vienna(event_silvester), Patna(event_diwali)

Performance: Weather + Air quality are fetched in parallel via ThreadPoolExecutor.
NASA POWER (soil) remains sequential due to stricter rate limits.
Estimated runtime: ~13h (vs ~20h fully sequential).
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

REQUEST_DELAY = 4   # seconds pause after each parallel weather+air burst
MAX_RETRIES   = 3

DATA_DIR = Path("raw_data_extended_cities")
DATA_DIR.mkdir(exist_ok=True)

CHECKPOINT_FILE = "checkpoint_extended.json"

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

air_params = (
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

TASK_ORDER = ["event", "ref_minus14", "ref_minus7", "ref_plus7"]

# ============================================================
# ALL ADDITIONAL CITIES (deduplicated - 152 unique entries)
# ============================================================

ADDITIONAL_CITIES = [

    # ----------------------------------------------------------------
    # 1. MEGACITIES - missing from existing scripts
    # ----------------------------------------------------------------

    # Asia
    {"category": "megacity", "country": "India",               "city": "Kolkata",         "lat":  22.5726,  "lon":  88.3639},
    {"category": "megacity", "country": "India",               "city": "Chennai",          "lat":  13.0827,  "lon":  80.2707},
    {"category": "megacity", "country": "India",               "city": "Bengaluru",        "lat":  12.9716,  "lon":  77.5946},
    {"category": "megacity", "country": "India",               "city": "Hyderabad",        "lat":  17.3850,  "lon":  78.4867},
    {"category": "megacity", "country": "India",               "city": "Ahmedabad",        "lat":  23.0225,  "lon":  72.5714},
    {"category": "megacity", "country": "Bangladesh",          "city": "Dhaka",            "lat":  23.8103,  "lon":  90.4125},
    {"category": "megacity", "country": "Pakistan",            "city": "Lahore",           "lat":  31.5497,  "lon":  74.3436},
    {"category": "megacity", "country": "Pakistan",            "city": "Faisalabad",       "lat":  31.4187,  "lon":  73.0791},
    {"category": "megacity", "country": "China",               "city": "Chongqing",        "lat":  29.5630,  "lon": 106.5516},
    {"category": "megacity", "country": "China",               "city": "Chengdu",          "lat":  30.5728,  "lon": 104.0668},
    {"category": "megacity", "country": "China",               "city": "Wuhan",            "lat":  30.5928,  "lon": 114.3055},
    {"category": "megacity", "country": "China",               "city": "Tianjin",          "lat":  39.3434,  "lon": 117.3616},
    {"category": "megacity", "country": "China",               "city": "Shenzhen",         "lat":  22.5431,  "lon": 114.0579},
    {"category": "megacity", "country": "China",               "city": "Xian",             "lat":  34.3416,  "lon": 108.9398},
    {"category": "megacity", "country": "China",               "city": "Nanjing",          "lat":  32.0603,  "lon": 118.7969},
    {"category": "megacity", "country": "China",               "city": "Shenyang",         "lat":  41.8057,  "lon": 123.4315},
    {"category": "megacity", "country": "Japan",               "city": "Osaka",            "lat":  34.6937,  "lon": 135.5023},
    {"category": "megacity", "country": "Japan",               "city": "Nagoya",           "lat":  35.1815,  "lon": 136.9066},
    {"category": "megacity", "country": "Japan",               "city": "Sapporo",          "lat":  43.0618,  "lon": 141.3545},
    {"category": "megacity", "country": "Indonesia",           "city": "Surabaya",         "lat":  -7.2575,  "lon": 112.7521},
    {"category": "megacity", "country": "Indonesia",           "city": "Bandung",          "lat":  -6.9175,  "lon": 107.6191},
    {"category": "megacity", "country": "Philippines",         "city": "Manila",           "lat":  14.5995,  "lon": 120.9842},
    {"category": "megacity", "country": "Viet Nam",            "city": "Ho Chi Minh City", "lat":  10.8231,  "lon": 106.6297},
    {"category": "megacity", "country": "Republic of Korea",   "city": "Busan",            "lat":  35.1796,  "lon": 129.0756},
    {"category": "megacity", "country": "Iran",                "city": "Mashhad",          "lat":  36.2972,  "lon":  59.6067},
    {"category": "megacity", "country": "Iran",                "city": "Isfahan",          "lat":  32.6546,  "lon":  51.6680},
    {"category": "megacity", "country": "Iraq",                "city": "Basra",            "lat":  30.5085,  "lon":  47.7804},
    {"category": "megacity", "country": "Saudi Arabia",        "city": "Jeddah",           "lat":  21.5433,  "lon":  39.1728},
    {"category": "megacity", "country": "Uzbekistan",          "city": "Tashkent",         "lat":  41.2995,  "lon":  69.2401},

    # Africa
    {"category": "megacity", "country": "Nigeria",             "city": "Lagos",            "lat":   6.5244,  "lon":   3.3792},
    {"category": "megacity", "country": "Nigeria",             "city": "Kano",             "lat":  12.0000,  "lon":   8.5167},
    {"category": "megacity", "country": "Nigeria",             "city": "Ibadan",           "lat":   7.3775,  "lon":   3.9470},
    {"category": "megacity", "country": "Ethiopia",            "city": "Addis Abeba",      "lat":   9.0301,  "lon":  38.7498},
    {"category": "megacity", "country": "DR Congo",            "city": "Kinshasa",         "lat":  -4.4419,  "lon":  15.2663},
    {"category": "megacity", "country": "Tanzania",            "city": "Dar es Salaam",    "lat":  -6.7924,  "lon":  39.2083},
    {"category": "megacity", "country": "Kenya",               "city": "Nairobi",          "lat":  -1.2921,  "lon":  36.8219},
    {"category": "megacity", "country": "Ghana",               "city": "Accra",            "lat":   5.6037,  "lon":  -0.1870},
    {"category": "megacity", "country": "Cameroon",            "city": "Douala",           "lat":   4.0511,  "lon":   9.7679},
    {"category": "megacity", "country": "South Africa",        "city": "Johannesburg",     "lat": -26.2041,  "lon":  28.0473},
    {"category": "megacity", "country": "South Africa",        "city": "Cape Town",        "lat": -33.9249,  "lon":  18.4241},
    {"category": "megacity", "country": "South Africa",        "city": "Durban",           "lat": -29.8587,  "lon":  31.0218},
    {"category": "megacity", "country": "Sudan",               "city": "Khartoum",         "lat":  15.5007,  "lon":  32.5599},
    {"category": "megacity", "country": "Algeria",             "city": "Algiers",          "lat":  36.7372,  "lon":   3.0863},
    {"category": "megacity", "country": "Morocco",             "city": "Rabat",            "lat":  34.0209,  "lon":  -6.8416},

    # Americas
    {"category": "megacity", "country": "Mexico",              "city": "Mexico City",      "lat":  19.4326,  "lon": -99.1332},
    {"category": "megacity", "country": "Mexico",              "city": "Guadalajara",      "lat":  20.6597,  "lon":-103.3496},
    {"category": "megacity", "country": "Mexico",              "city": "Monterrey",        "lat":  25.6866,  "lon":-100.3161},
    {"category": "megacity", "country": "Brazil",              "city": "Belo Horizonte",   "lat": -19.9191,  "lon": -43.9386},
    {"category": "megacity", "country": "Brazil",              "city": "Fortaleza",        "lat":  -3.7319,  "lon": -38.5267},
    {"category": "megacity", "country": "Brazil",              "city": "Recife",           "lat":  -8.0578,  "lon": -34.8829},
    {"category": "megacity", "country": "Brazil",              "city": "Salvador",         "lat": -12.9714,  "lon": -38.5014},
    {"category": "megacity", "country": "Brazil",              "city": "Porto Alegre",     "lat": -30.0346,  "lon": -51.2177},
    {"category": "megacity", "country": "Brazil",              "city": "Manaus",           "lat":  -3.1190,  "lon": -60.0217},
    {"category": "megacity", "country": "Colombia",            "city": "Bogota",           "lat":   4.7110,  "lon": -74.0721},
    {"category": "megacity", "country": "Colombia",            "city": "Medellin",         "lat":   6.2476,  "lon": -75.5658},
    {"category": "megacity", "country": "Argentina",           "city": "Buenos Aires",     "lat": -34.6037,  "lon": -58.3816},
    {"category": "megacity", "country": "Argentina",           "city": "Cordoba",          "lat": -31.4201,  "lon": -64.1888},
    {"category": "megacity", "country": "Peru",                "city": "Lima",             "lat": -12.0464,  "lon": -77.0428},
    {"category": "megacity", "country": "Venezuela",           "city": "Caracas",          "lat":  10.4806,  "lon": -66.9036},
    {"category": "megacity", "country": "Chile",               "city": "Santiago",         "lat": -33.4489,  "lon": -70.6693},
    {"category": "megacity", "country": "United States",       "city": "Houston",          "lat":  29.7604,  "lon": -95.3698},
    {"category": "megacity", "country": "United States",       "city": "Phoenix",          "lat":  33.4484,  "lon":-112.0740},
    {"category": "megacity", "country": "United States",       "city": "Philadelphia",     "lat":  39.9526,  "lon": -75.1652},
    {"category": "megacity", "country": "United States",       "city": "San Antonio",      "lat":  29.4241,  "lon": -98.4936},
    {"category": "megacity", "country": "United States",       "city": "Dallas",           "lat":  32.7767,  "lon": -96.7970},
    {"category": "megacity", "country": "United States",       "city": "Miami",            "lat":  25.7617,  "lon": -80.1918},
    {"category": "megacity", "country": "United States",       "city": "Atlanta",          "lat":  33.7490,  "lon": -84.3880},
    {"category": "megacity", "country": "United States",       "city": "Seattle",          "lat":  47.6062,  "lon":-122.3321},
    {"category": "megacity", "country": "United States",       "city": "Boston",           "lat":  42.3601,  "lon": -71.0589},
    {"category": "megacity", "country": "Canada",              "city": "Montreal",         "lat":  45.5017,  "lon": -73.5673},
    {"category": "megacity", "country": "Canada",              "city": "Calgary",          "lat":  51.0447,  "lon":-114.0719},

    # Europe
    {"category": "megacity", "country": "Russian Federation",  "city": "St. Petersburg",  "lat":  59.9311,  "lon":  30.3609},
    {"category": "megacity", "country": "Russian Federation",  "city": "Novosibirsk",     "lat":  54.9885,  "lon":  82.9327},
    {"category": "megacity", "country": "Russian Federation",  "city": "Yekaterinburg",   "lat":  56.8389,  "lon":  60.6057},
    {"category": "megacity", "country": "Ukraine",             "city": "Kharkiv",         "lat":  49.9935,  "lon":  36.2304},
    {"category": "megacity", "country": "Spain",               "city": "Barcelona",       "lat":  41.3851,  "lon":   2.1734},
    {"category": "megacity", "country": "Spain",               "city": "Madrid",          "lat":  40.4168,  "lon":  -3.7038},
    {"category": "megacity", "country": "Spain",               "city": "Seville",         "lat":  37.3886,  "lon":  -5.9823},
    {"category": "megacity", "country": "Italy",               "city": "Milan",           "lat":  45.4654,  "lon":   9.1859},
    {"category": "megacity", "country": "Italy",               "city": "Naples",          "lat":  40.8518,  "lon":  14.2681},
    {"category": "megacity", "country": "Germany",             "city": "Hamburg",         "lat":  53.5753,  "lon":   9.9950},
    {"category": "megacity", "country": "Germany",             "city": "Munich",          "lat":  48.1351,  "lon":  11.5820},
    {"category": "megacity", "country": "Germany",             "city": "Frankfurt",       "lat":  50.1109,  "lon":   8.6821},
    {"category": "megacity", "country": "Poland",              "city": "Warsaw",          "lat":  52.2297,  "lon":  21.0122},
    {"category": "megacity", "country": "Poland",              "city": "Krakow",          "lat":  50.0647,  "lon":  19.9450},
    {"category": "megacity", "country": "Netherlands",         "city": "Amsterdam",       "lat":  52.3676,  "lon":   4.9041},
    {"category": "megacity", "country": "Netherlands",         "city": "Rotterdam",       "lat":  51.9244,  "lon":   4.4777},
    {"category": "megacity", "country": "Sweden",              "city": "Stockholm",       "lat":  59.3293,  "lon":  18.0686},
    {"category": "megacity", "country": "Sweden",              "city": "Gothenburg",      "lat":  57.7089,  "lon":  11.9746},
    {"category": "megacity", "country": "Switzerland",         "city": "Zurich",          "lat":  47.3769,  "lon":   8.5417},
    {"category": "megacity", "country": "Austria",             "city": "Vienna",          "lat":  48.2082,  "lon":  16.3738},
    {"category": "megacity", "country": "Greece",              "city": "Athens",          "lat":  37.9838,  "lon":  23.7275},
    {"category": "megacity", "country": "Romania",             "city": "Bucharest",       "lat":  44.4268,  "lon":  26.1025},
    {"category": "megacity", "country": "Hungary",             "city": "Budapest",        "lat":  47.4979,  "lon":  19.0402},
    {"category": "megacity", "country": "Portugal",            "city": "Lisbon",          "lat":  38.7169,  "lon":  -9.1390},
    {"category": "megacity", "country": "Denmark",             "city": "Copenhagen",      "lat":  55.6761,  "lon":  12.5683},
    {"category": "megacity", "country": "Norway",              "city": "Oslo",            "lat":  59.9139,  "lon":  10.7522},
    {"category": "megacity", "country": "Finland",             "city": "Helsinki",        "lat":  60.1699,  "lon":  24.9384},
    {"category": "megacity", "country": "Czechia",             "city": "Prague",          "lat":  50.0755,  "lon":  14.4378},
    {"category": "megacity", "country": "Belgium",             "city": "Brussels",        "lat":  50.8503,  "lon":   4.3517},

    # ----------------------------------------------------------------
    # 2. LARGE COUNTRIES - secondary cities for geographic coverage
    # ----------------------------------------------------------------

    # Russia
    {"category": "large_country", "country": "Russian Federation", "city": "Vladivostok", "lat":  43.1332, "lon": 131.9113},
    {"category": "large_country", "country": "Russian Federation", "city": "Omsk",         "lat":  54.9885, "lon":  73.3242},
    {"category": "large_country", "country": "Russian Federation", "city": "Krasnoyarsk",  "lat":  56.0153, "lon":  92.8932},
    {"category": "large_country", "country": "Russian Federation", "city": "Irkutsk",       "lat":  52.2978, "lon": 104.2964},

    # USA
    {"category": "large_country", "country": "United States", "city": "Anchorage",   "lat":  61.2181, "lon":-149.9003},
    {"category": "large_country", "country": "United States", "city": "Honolulu",    "lat":  21.3069, "lon":-157.8583},
    {"category": "large_country", "country": "United States", "city": "Denver",      "lat":  39.7392, "lon":-104.9903},
    {"category": "large_country", "country": "United States", "city": "Minneapolis", "lat":  44.9778, "lon": -93.2650},
    {"category": "large_country", "country": "United States", "city": "New Orleans", "lat":  29.9511, "lon": -90.0715},

    # Australia
    {"category": "large_country", "country": "Australia", "city": "Brisbane", "lat": -27.4698, "lon": 153.0251},
    {"category": "large_country", "country": "Australia", "city": "Perth",    "lat": -31.9505, "lon": 115.8605},
    {"category": "large_country", "country": "Australia", "city": "Adelaide", "lat": -34.9285, "lon": 138.6007},
    {"category": "large_country", "country": "Australia", "city": "Darwin",   "lat": -12.4634, "lon": 130.8456},

    # China
    {"category": "large_country", "country": "China", "city": "Urumqi",  "lat":  43.8256, "lon":  87.6168},
    {"category": "large_country", "country": "China", "city": "Lhasa",   "lat":  29.6500, "lon":  91.1000},
    {"category": "large_country", "country": "China", "city": "Harbin",  "lat":  45.8038, "lon": 126.5349},
    {"category": "large_country", "country": "China", "city": "Kunming", "lat":  25.0389, "lon": 102.7183},

    # Brazil
    {"category": "large_country", "country": "Brazil", "city": "Belem",    "lat":  -1.4558, "lon": -48.5044},
    {"category": "large_country", "country": "Brazil", "city": "Curitiba", "lat": -25.4290, "lon": -49.2671},

    # Canada
    {"category": "large_country", "country": "Canada", "city": "Edmonton",   "lat":  53.5461, "lon":-113.4938},
    {"category": "large_country", "country": "Canada", "city": "Winnipeg",   "lat":  49.8951, "lon": -97.1384},
    {"category": "large_country", "country": "Canada", "city": "Quebec City","lat":  46.8139, "lon": -71.2080},
    {"category": "large_country", "country": "Canada", "city": "Halifax",    "lat":  44.6488, "lon": -63.5752},

    # India
    {"category": "large_country", "country": "India", "city": "Pune",   "lat":  18.5204, "lon":  73.8567},
    {"category": "large_country", "country": "India", "city": "Surat",  "lat":  21.1702, "lon":  72.8311},
    {"category": "large_country", "country": "India", "city": "Patna",  "lat":  25.5941, "lon":  85.1376},

    # ----------------------------------------------------------------
    # 3. EVENT SUPPLEMENTS - unique cities not already covered above
    # ----------------------------------------------------------------

    # Silvester
    {"category": "event_silvester", "country": "United Kingdom", "city": "Edinburgh", "lat":  55.9533, "lon":  -3.1883},
    {"category": "event_silvester", "country": "Italy",          "city": "Rome",      "lat":  41.9028, "lon":  12.4964},

    # Diwali supplements
    {"category": "event_diwali", "country": "India",          "city": "Ayodhya",  "lat":  26.7922, "lon":  82.1998},
    {"category": "event_diwali", "country": "United Kingdom", "city": "Leicester","lat":  52.6369, "lon":  -1.1398},
    {"category": "event_diwali", "country": "Fiji",           "city": "Suva",     "lat": -18.1416, "lon": 178.4419},

    # Chinese New Year supplements
    {"category": "event_cny", "country": "Taiwan",        "city": "Taipei",   "lat":  25.0330, "lon": 121.5654},
    {"category": "event_cny", "country": "Indonesia",     "city": "Medan",    "lat":   3.5952, "lon":  98.6722},
    {"category": "event_cny", "country": "Thailand",      "city": "Bangkok",  "lat":  13.7563, "lon": 100.5018},
    {"category": "event_cny", "country": "Australia",     "city": "Sydney",   "lat": -33.8688, "lon": 151.2093},
    {"category": "event_cny", "country": "United Kingdom","city": "London",   "lat":  51.5074, "lon":  -0.1278},

    # Bonfire Night supplements
    {"category": "event_bonfire", "country": "United Kingdom", "city": "Leeds",      "lat":  53.8008, "lon":  -1.5491},
    {"category": "event_bonfire", "country": "United Kingdom", "city": "Bristol",    "lat":  51.4545, "lon":  -2.5879},
    {"category": "event_bonfire", "country": "New Zealand",    "city": "Auckland",   "lat": -36.8485, "lon": 174.7633},
    {"category": "event_bonfire", "country": "New Zealand",    "city": "Wellington", "lat": -41.2865, "lon": 174.7762},

    # Eid supplements
    {"category": "event_eid", "country": "Iraq",        "city": "Baghdad",   "lat":  33.3152, "lon":  44.3661},
    {"category": "event_eid", "country": "Libya",       "city": "Tripoli",   "lat":  32.8872, "lon":  13.1913},
    {"category": "event_eid", "country": "Jordan",      "city": "Amman",     "lat":  31.9539, "lon":  35.9106},
    {"category": "event_eid", "country": "Somalia",     "city": "Mogadishu", "lat":   2.0469, "lon":  45.3182},
    {"category": "event_eid", "country": "Afghanistan", "city": "Kabul",     "lat":  34.5553, "lon":  69.2075},

    # Loy Krathong supplements
    {"category": "event_loy", "country": "Thailand", "city": "Lampang", "lat":  18.2888, "lon":  99.4928},
    {"category": "event_loy", "country": "Thailand", "city": "Hat Yai", "lat":   7.0085, "lon": 100.4747},

    # Japan fireworks
    {"category": "event_japan_fw", "country": "Japan", "city": "Kyoto",   "lat":  35.0116, "lon": 135.7681},
    {"category": "event_japan_fw", "country": "Japan", "city": "Omagari", "lat":  39.4636, "lon": 140.4792},

    # Independence Day USA supplements
    {"category": "event_july4", "country": "United States", "city": "San Diego", "lat":  32.7157, "lon":-117.1611},
    {"category": "event_july4", "country": "United States", "city": "Nashville", "lat":  36.1627, "lon": -86.7816},

    # Malta
    {"category": "event_malta", "country": "Malta", "city": "Mdina", "lat":  35.8878, "lon":  14.4036},
    {"category": "event_malta", "country": "Malta", "city": "Gozo",  "lat":  36.0449, "lon":  14.2500},
]

# ============================================================
# CHECKPOINT
# ============================================================

def load_checkpoint():
    if Path(CHECKPOINT_FILE).exists():
        return json.load(open(CHECKPOINT_FILE))
    return {"city_idx": 0, "year": None, "data_type": None}

def save_checkpoint(city_idx, year, data_type):
    json.dump({"city_idx": city_idx, "year": year, "data_type": data_type},
              open(CHECKPOINT_FILE, "w"))

# ============================================================
# API HELPERS
# ============================================================

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
    raise RuntimeError(f"API failed after {MAX_RETRIES} retries: {url}")


def fetch_weather_and_air_parallel(lat, lon, start_str, end_str):
    """
    Fetch weather and air quality simultaneously using two threads.
    Both endpoints are from Open-Meteo (different hosts) so parallel calls are safe.
    Returns (weather_json, air_json).
    """
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
    """Fetch soil data from NASA POWER (sequential – stricter rate limits)."""
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

# ============================================================
# WRITE
# ============================================================

def write_rows(city, data_type, weather, air, soil):
    csv_path = DATA_DIR / f"{city['category']}.csv"
    file_exists = csv_path.exists()

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

# ============================================================
# MAIN
# ============================================================

checkpoint  = load_checkpoint()
start_idx   = checkpoint.get("city_idx", 0)
resume_year = checkpoint.get("year")
resume_type = checkpoint.get("data_type")
resume_mode = resume_year is not None

for city_idx, city in enumerate(ADDITIONAL_CITIES):

    if city_idx < start_idx:
        continue

    lat = city["lat"]
    lon = city["lon"]
    logging.info(f"[{city_idx+1}/{len(ADDITIONAL_CITIES)}] {city['city']} ({city['country']}) - {city['category']}")

    for year in YEARS:

        # Resume: skip years before checkpoint
        if resume_mode and year < resume_year:
            continue

        event_date = date(year, 12, 31)

        tasks = [
            ("event",       event_date - timedelta(days=1),  event_date + timedelta(days=1)),
            ("ref_minus14", event_date - timedelta(days=15), event_date - timedelta(days=13)),
            ("ref_minus7",  event_date - timedelta(days=8),  event_date - timedelta(days=6)),
            ("ref_plus7",   event_date + timedelta(days=6),  event_date + timedelta(days=8)),
        ]

        for data_type, start_d, end_d in tasks:

            # Resume: skip tasks before checkpoint within the resume year
            if resume_mode and year == resume_year:
                if TASK_ORDER.index(data_type) < TASK_ORDER.index(resume_type):
                    continue

            start_str = start_d.strftime("%Y-%m-%d")
            end_str   = end_d.strftime("%Y-%m-%d")

            try:
                # ── Parallel: Weather + Air (different Open-Meteo endpoints) ──
                weather, air = fetch_weather_and_air_parallel(lat, lon, start_str, end_str)
                time.sleep(REQUEST_DELAY)

                # ── Sequential: Soil (NASA POWER - stricter rate limits) ───────
                soil = fetch_soil(lat, lon, start_str, end_str)
                time.sleep(REQUEST_DELAY)

                write_rows(city, data_type, weather, air, soil)
                save_checkpoint(city_idx, year, data_type)
                resume_mode = False

            except Exception as e:
                logging.error(f"{city['city']} {year} {data_type} failed: {e}")

    time.sleep(2)

logging.info("Extended cities data collection complete.")