"""
elevation_collector.py
=======================
Collects elevation above sea level (meters) for all locations
used across all collector scripts.
Source: Open-Meteo Elevation API (Copernicus DEM GLO-90, 90m resolution)
No API key required. Run once - elevation is static data.

Note: Cities that serve as both capitals and event cities
(Ottawa, Bangkok, Dublin, Jakarta, Kuala Lumpur, Riyadh,
Abu Dhabi, Valletta, Singapore, Hanoi, Seoul, Vientiane,
Cairo, Istanbul, Kathmandu) are listed once as capitals only.
"""

import requests
import time
import logging
import csv
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------- CONFIG ----------------

ELEVATION_URL = "https://api.open-meteo.com/v1/elevation"

REQUEST_DELAY = 1
MAX_RETRIES   = 3

DATA_DIR = Path("raw_data_population")
DATA_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = DATA_DIR / "elevation_all_locations.csv"

FIELDNAMES = ["location_type", "name", "country", "latitude", "longitude", "elevation_m"]

# All locations: capitals + event cities
# Cities already covered as capitals are NOT repeated in event cities section
ALL_LOCATIONS = [
    # ---- Silvester capitals ----
    {"type": "capital", "name": "Kabul",              "country": "Afghanistan",              "lat":  34.5553,  "lon":  69.2075},
    {"type": "capital", "name": "Tirana",             "country": "Albania",                  "lat":  41.3275,  "lon":  19.8189},
    {"type": "capital", "name": "Algier",             "country": "Algeria",                  "lat":  36.7372,  "lon":   3.0863},
    {"type": "capital", "name": "Luanda",             "country": "Angola",                   "lat":  -8.8383,  "lon":  13.2344},
    {"type": "capital", "name": "Buenos Aires",       "country": "Argentina",                "lat": -34.6037,  "lon": -58.3816},
    {"type": "capital", "name": "Jerewan",            "country": "Armenia",                  "lat":  40.1792,  "lon":  44.4991},
    {"type": "capital", "name": "Canberra",           "country": "Australia",                "lat": -35.2809,  "lon": 149.1300},
    {"type": "capital", "name": "Wien",               "country": "Austria",                  "lat":  48.2082,  "lon":  16.3738},
    {"type": "capital", "name": "Baku",               "country": "Azerbaijan",               "lat":  40.4093,  "lon":  49.8671},
    {"type": "capital", "name": "Manama",             "country": "Bahrain",                  "lat":  26.2285,  "lon":  50.5861},
    {"type": "capital", "name": "Dhaka",              "country": "Bangladesh",               "lat":  23.8103,  "lon":  90.4125},
    {"type": "capital", "name": "Minsk",              "country": "Belarus",                  "lat":  53.9006,  "lon":  27.5590},
    {"type": "capital", "name": "Brüssel",            "country": "Belgium",                  "lat":  50.8503,  "lon":   4.3517},
    {"type": "capital", "name": "Belmopan",           "country": "Belize",                   "lat":  17.2514,  "lon": -88.7660},
    {"type": "capital", "name": "Porto-Novo",         "country": "Benin",                    "lat":   6.4969,  "lon":   2.6289},
    {"type": "capital", "name": "Thimphu",            "country": "Bhutan",                   "lat":  27.4728,  "lon":  89.6390},
    {"type": "capital", "name": "Sucre",              "country": "Bolivia",                  "lat": -19.0196,  "lon": -65.2619},
    {"type": "capital", "name": "Sarajevo",           "country": "Bosnia and Herzegovina",   "lat":  43.8563,  "lon":  18.4131},
    {"type": "capital", "name": "Gaborone",           "country": "Botswana",                 "lat": -24.6282,  "lon":  25.9231},
    {"type": "capital", "name": "Brasília",           "country": "Brazil",                   "lat": -15.7939,  "lon": -47.8828},
    {"type": "capital", "name": "Sofia",              "country": "Bulgaria",                 "lat":  42.6977,  "lon":  23.3219},
    {"type": "capital", "name": "Ouagadougou",        "country": "Burkina Faso",             "lat":  12.3714,  "lon":  -1.5197},
    {"type": "capital", "name": "Gitega",             "country": "Burundi",                  "lat":  -3.4264,  "lon":  29.9306},
    {"type": "capital", "name": "Phnom Penh",         "country": "Cambodia",                 "lat":  11.5564,  "lon": 104.9282},
    {"type": "capital", "name": "Yaoundé",            "country": "Cameroon",                 "lat":   3.8480,  "lon":  11.5021},
    {"type": "capital", "name": "Ottawa",             "country": "Canada",                   "lat":  45.4215,  "lon": -75.6972},
    {"type": "capital", "name": "Bangui",             "country": "Central African Republic", "lat":   4.3947,  "lon":  18.5582},
    {"type": "capital", "name": "N'Djamena",          "country": "Chad",                     "lat":  12.1348,  "lon":  15.0557},
    {"type": "capital", "name": "Santiago",           "country": "Chile",                    "lat": -33.4489,  "lon": -70.6693},
    {"type": "capital", "name": "Beijing",            "country": "China",                    "lat":  39.9042,  "lon": 116.4074},
    {"type": "capital", "name": "Bogotá",             "country": "Colombia",                 "lat":   4.7110,  "lon": -74.0721},
    {"type": "capital", "name": "Moroni",             "country": "Comoros",                  "lat": -11.7172,  "lon":  43.2473},
    {"type": "capital", "name": "Brazzaville",        "country": "Congo",                    "lat":  -4.2634,  "lon":  15.2429},
    {"type": "capital", "name": "San José",           "country": "Costa Rica",               "lat":   9.9281,  "lon": -84.0907},
    {"type": "capital", "name": "Zagreb",             "country": "Croatia",                  "lat":  45.8150,  "lon":  15.9819},
    {"type": "capital", "name": "Havanna",            "country": "Cuba",                     "lat":  23.1136,  "lon": -82.3666},
    {"type": "capital", "name": "Nikosia",            "country": "Cyprus",                   "lat":  35.1856,  "lon":  33.3823},
    {"type": "capital", "name": "Prag",               "country": "Czechia",                  "lat":  50.0755,  "lon":  14.4378},
    {"type": "capital", "name": "Kopenhagen",         "country": "Denmark",                  "lat":  55.6761,  "lon":  12.5683},
    {"type": "capital", "name": "Kinshasa",           "country": "DR Congo",                 "lat":  -4.4419,  "lon":  15.2663},
    {"type": "capital", "name": "Quito",              "country": "Ecuador",                  "lat":  -0.2295,  "lon": -78.5243},
    {"type": "capital", "name": "Kairo",              "country": "Egypt",                    "lat":  30.0444,  "lon":  31.2357},
    {"type": "capital", "name": "San Salvador",       "country": "El Salvador",              "lat":  13.6929,  "lon": -89.2182},
    {"type": "capital", "name": "Tallinn",            "country": "Estonia",                  "lat":  59.4370,  "lon":  24.7535},
    {"type": "capital", "name": "Mbabane",            "country": "Eswatini",                 "lat": -26.3167,  "lon":  31.1333},
    {"type": "capital", "name": "Addis Abeba",        "country": "Ethiopia",                 "lat":   9.0301,  "lon":  38.7498},
    {"type": "capital", "name": "Helsinki",           "country": "Finland",                  "lat":  60.1699,  "lon":  24.9384},
    {"type": "capital", "name": "Paris",              "country": "France",                   "lat":  48.8566,  "lon":   2.3522},
    {"type": "capital", "name": "Libreville",         "country": "Gabon",                    "lat":   0.4162,  "lon":   9.4673},
    {"type": "capital", "name": "Banjul",             "country": "Gambia",                   "lat":  13.4549,  "lon": -16.5790},
    {"type": "capital", "name": "Tiflis",             "country": "Georgia",                  "lat":  41.7151,  "lon":  44.8271},
    {"type": "capital", "name": "Berlin",             "country": "Germany",                  "lat":  52.5200,  "lon":  13.4050},
    {"type": "capital", "name": "Accra",              "country": "Ghana",                    "lat":   5.6037,  "lon":  -0.1870},
    {"type": "capital", "name": "Athen",              "country": "Greece",                   "lat":  37.9838,  "lon":  23.7275},
    {"type": "capital", "name": "Guatemala-Stadt",    "country": "Guatemala",                "lat":  14.6349,  "lon": -90.5069},
    {"type": "capital", "name": "Conakry",            "country": "Guinea",                   "lat":   9.6412,  "lon": -13.5784},
    {"type": "capital", "name": "Georgetown",         "country": "Guyana",                   "lat":   6.8013,  "lon": -58.1551},
    {"type": "capital", "name": "Port-au-Prince",     "country": "Haiti",                    "lat":  18.5944,  "lon": -72.3074},
    {"type": "capital", "name": "Tegucigalpa",        "country": "Honduras",                 "lat":  14.0723,  "lon": -87.1921},
    {"type": "capital", "name": "Budapest",           "country": "Hungary",                  "lat":  47.4979,  "lon":  19.0402},
    {"type": "capital", "name": "Reykjavik",          "country": "Iceland",                  "lat":  64.1355,  "lon": -21.8954},
    {"type": "capital", "name": "Neu-Delhi",          "country": "India",                    "lat":  28.6139,  "lon":  77.2090},
    {"type": "capital", "name": "Jakarta",            "country": "Indonesia",                "lat":  -6.2088,  "lon": 106.8456},
    {"type": "capital", "name": "Teheran",            "country": "Iran",                     "lat":  35.6892,  "lon":  51.3890},
    {"type": "capital", "name": "Bagdad",             "country": "Iraq",                     "lat":  33.3152,  "lon":  44.3661},
    {"type": "capital", "name": "Dublin",             "country": "Ireland",                  "lat":  53.3331,  "lon":  -6.2489},
    {"type": "capital", "name": "Jerusalem",          "country": "Israel",                   "lat":  31.7683,  "lon":  35.2137},
    {"type": "capital", "name": "Rom",                "country": "Italy",                    "lat":  41.9028,  "lon":  12.4964},
    {"type": "capital", "name": "Kingston",           "country": "Jamaica",                  "lat":  17.9714,  "lon": -76.7936},
    {"type": "capital", "name": "Tokio",              "country": "Japan",                    "lat":  35.6895,  "lon": 139.6917},
    {"type": "capital", "name": "Amman",              "country": "Jordan",                   "lat":  31.9539,  "lon":  35.9106},
    {"type": "capital", "name": "Astana",             "country": "Kazakhstan",               "lat":  51.1694,  "lon":  71.4491},
    {"type": "capital", "name": "Nairobi",            "country": "Kenya",                    "lat":  -1.2921,  "lon":  36.8219},
    {"type": "capital", "name": "Kuwait-Stadt",       "country": "Kuwait",                   "lat":  29.3759,  "lon":  47.9774},
    {"type": "capital", "name": "Bischkek",           "country": "Kyrgyzstan",               "lat":  42.8746,  "lon":  74.5698},
    {"type": "capital", "name": "Vientiane",          "country": "Lao PDR",                  "lat":  17.9757,  "lon": 102.6331},
    {"type": "capital", "name": "Riga",               "country": "Latvia",                   "lat":  56.9496,  "lon":  24.1052},
    {"type": "capital", "name": "Beirut",             "country": "Lebanon",                  "lat":  33.8938,  "lon":  35.5018},
    {"type": "capital", "name": "Maseru",             "country": "Lesotho",                  "lat": -29.3158,  "lon":  27.4854},
    {"type": "capital", "name": "Monrovia",           "country": "Liberia",                  "lat":   6.3156,  "lon": -10.8074},
    {"type": "capital", "name": "Tripolis",           "country": "Libya",                    "lat":  32.8872,  "lon":  13.1913},
    {"type": "capital", "name": "Vilnius",            "country": "Lithuania",                "lat":  54.6872,  "lon":  25.2797},
    {"type": "capital", "name": "Luxemburg",          "country": "Luxembourg",               "lat":  49.6117,  "lon":   6.1319},
    {"type": "capital", "name": "Antananarivo",       "country": "Madagascar",               "lat": -18.8792,  "lon":  47.5079},
    {"type": "capital", "name": "Lilongwe",           "country": "Malawi",                   "lat": -13.9626,  "lon":  33.7741},
    {"type": "capital", "name": "Kuala Lumpur",       "country": "Malaysia",                 "lat":   3.1390,  "lon": 101.6869},
    {"type": "capital", "name": "Malé",               "country": "Maldives",                 "lat":   4.1755,  "lon":  73.5093},
    {"type": "capital", "name": "Bamako",             "country": "Mali",                     "lat":  12.6392,  "lon":  -8.0029},
    {"type": "capital", "name": "Valletta",           "country": "Malta",                    "lat":  35.8997,  "lon":  14.5146},
    {"type": "capital", "name": "Nouakchott",         "country": "Mauritania",               "lat":  18.0783,  "lon": -15.9744},
    {"type": "capital", "name": "Port Louis",         "country": "Mauritius",                "lat": -20.1669,  "lon":  57.5023},
    {"type": "capital", "name": "Mexiko-Stadt",       "country": "Mexico",                   "lat":  19.4326,  "lon": -99.1332},
    {"type": "capital", "name": "Ulaanbaatar",        "country": "Mongolia",                 "lat":  47.8864,  "lon": 106.9057},
    {"type": "capital", "name": "Podgorica",          "country": "Montenegro",               "lat":  42.4410,  "lon":  19.2627},
    {"type": "capital", "name": "Rabat",              "country": "Morocco",                  "lat":  34.0209,  "lon":  -6.8416},
    {"type": "capital", "name": "Maputo",             "country": "Mozambique",               "lat": -25.9653,  "lon":  32.5892},
    {"type": "capital", "name": "Naypyidaw",          "country": "Myanmar",                  "lat":  19.7633,  "lon":  96.0785},
    {"type": "capital", "name": "Windhoek",           "country": "Namibia",                  "lat": -22.5609,  "lon":  17.0658},
    {"type": "capital", "name": "Kathmandu",          "country": "Nepal",                    "lat":  27.7172,  "lon":  85.3240},
    {"type": "capital", "name": "Amsterdam",          "country": "Netherlands",              "lat":  52.3676,  "lon":   4.9041},
    {"type": "capital", "name": "Wellington",         "country": "New Zealand",              "lat": -41.2865,  "lon": 174.7762},
    {"type": "capital", "name": "Managua",            "country": "Nicaragua",                "lat":  12.1140,  "lon": -86.2362},
    {"type": "capital", "name": "Niamey",             "country": "Niger",                    "lat":  13.5126,  "lon":   2.1125},
    {"type": "capital", "name": "Abuja",              "country": "Nigeria",                  "lat":   9.0579,  "lon":   7.4951},
    {"type": "capital", "name": "Skopje",             "country": "North Macedonia",          "lat":  41.9981,  "lon":  21.4254},
    {"type": "capital", "name": "Oslo",               "country": "Norway",                   "lat":  59.9139,  "lon":  10.7522},
    {"type": "capital", "name": "Maskat",             "country": "Oman",                     "lat":  23.5880,  "lon":  58.3829},
    {"type": "capital", "name": "Islamabad",          "country": "Pakistan",                 "lat":  33.6844,  "lon":  73.0479},
    {"type": "capital", "name": "Panama-Stadt",       "country": "Panama",                   "lat":   8.9833,  "lon": -79.5167},
    {"type": "capital", "name": "Asunción",           "country": "Paraguay",                 "lat": -25.2637,  "lon": -57.5759},
    {"type": "capital", "name": "Lima",               "country": "Peru",                     "lat": -12.0464,  "lon": -77.0428},
    {"type": "capital", "name": "Manila",             "country": "Philippines",              "lat":  14.5995,  "lon": 120.9842},
    {"type": "capital", "name": "Warschau",           "country": "Poland",                   "lat":  52.2297,  "lon":  21.0122},
    {"type": "capital", "name": "Lissabon",           "country": "Portugal",                 "lat":  38.7169,  "lon":  -9.1390},
    {"type": "capital", "name": "Doha",               "country": "Qatar",                    "lat":  25.2854,  "lon":  51.5310},
    {"type": "capital", "name": "Seoul",              "country": "Republic of Korea",        "lat":  37.5665,  "lon": 126.9780},
    {"type": "capital", "name": "Chisinau",           "country": "Republic of Moldova",      "lat":  47.0105,  "lon":  28.8638},
    {"type": "capital", "name": "Bukarest",           "country": "Romania",                  "lat":  44.4268,  "lon":  26.1025},
    {"type": "capital", "name": "Moskau",             "country": "Russian Federation",       "lat":  55.7558,  "lon":  37.6173},
    {"type": "capital", "name": "Kigali",             "country": "Rwanda",                   "lat":  -1.9579,  "lon":  30.1127},
    {"type": "capital", "name": "Riyadh",             "country": "Saudi Arabia",             "lat":  24.7136,  "lon":  46.6753},
    {"type": "capital", "name": "Dakar",              "country": "Senegal",                  "lat":  14.6928,  "lon": -17.4467},
    {"type": "capital", "name": "Belgrad",            "country": "Serbia",                   "lat":  44.8176,  "lon":  20.4569},
    {"type": "capital", "name": "Freetown",           "country": "Sierra Leone",             "lat":   8.4657,  "lon": -13.2317},
    {"type": "capital", "name": "Singapur",           "country": "Singapore",                "lat":   1.3521,  "lon": 103.8198},
    {"type": "capital", "name": "Bratislava",         "country": "Slovakia",                 "lat":  48.1486,  "lon":  17.1077},
    {"type": "capital", "name": "Ljubljana",          "country": "Slovenia",                 "lat":  46.0569,  "lon":  14.5058},
    {"type": "capital", "name": "Mogadischu",         "country": "Somalia",                  "lat":   2.0469,  "lon":  45.3182},
    {"type": "capital", "name": "Pretoria",           "country": "South Africa",             "lat": -25.7479,  "lon":  28.2293},
    {"type": "capital", "name": "Juba",               "country": "South Sudan",              "lat":   4.8594,  "lon":  31.5713},
    {"type": "capital", "name": "Madrid",             "country": "Spain",                    "lat":  40.4168,  "lon":  -3.7038},
    {"type": "capital", "name": "Sri Jayawardenepura","country": "Sri Lanka",                "lat":   6.9271,  "lon":  79.8612},
    {"type": "capital", "name": "Khartum",            "country": "Sudan",                    "lat":  15.5007,  "lon":  32.5599},
    {"type": "capital", "name": "Paramaribo",         "country": "Suriname",                 "lat":   5.8520,  "lon": -55.2038},
    {"type": "capital", "name": "Stockholm",          "country": "Sweden",                   "lat":  59.3293,  "lon":  18.0686},
    {"type": "capital", "name": "Damaskus",           "country": "Syria",                    "lat":  33.5138,  "lon":  36.2765},
    {"type": "capital", "name": "Duschanbe",          "country": "Tajikistan",               "lat":  38.5598,  "lon":  68.7870},
    {"type": "capital", "name": "Dodoma",             "country": "Tanzania",                 "lat":  -6.1630,  "lon":  35.7516},
    {"type": "capital", "name": "Bangkok",            "country": "Thailand",                 "lat":  13.7563,  "lon": 100.5018},
    {"type": "capital", "name": "Lomé",               "country": "Togo",                     "lat":   6.1725,  "lon":   1.2314},
    {"type": "capital", "name": "Port of Spain",      "country": "Trinidad and Tobago",      "lat":  10.6600,  "lon": -61.5085},
    {"type": "capital", "name": "Tunis",              "country": "Tunisia",                  "lat":  36.8065,  "lon":  10.1815},
    {"type": "capital", "name": "Ankara",             "country": "Türkiye",                  "lat":  39.9208,  "lon":  32.8541},
    {"type": "capital", "name": "Aschgabat",          "country": "Turkmenistan",             "lat":  37.9601,  "lon":  58.3261},
    {"type": "capital", "name": "Kampala",            "country": "Uganda",                   "lat":   0.3476,  "lon":  32.5825},
    {"type": "capital", "name": "Kiew",               "country": "Ukraine",                  "lat":  50.4501,  "lon":  30.5234},
    {"type": "capital", "name": "Abu Dhabi",          "country": "United Arab Emirates",     "lat":  24.4539,  "lon":  54.3773},
    {"type": "capital", "name": "London",             "country": "United Kingdom",           "lat":  51.5074,  "lon":  -0.1278},
    {"type": "capital", "name": "Washington DC",      "country": "United States",            "lat":  38.8951,  "lon": -77.0364},
    {"type": "capital", "name": "Montevideo",         "country": "Uruguay",                  "lat": -34.9011,  "lon": -56.1645},
    {"type": "capital", "name": "Taschkent",          "country": "Uzbekistan",               "lat":  41.2995,  "lon":  69.2401},
    {"type": "capital", "name": "Caracas",            "country": "Venezuela",                "lat":  10.4806,  "lon": -66.9036},
    {"type": "capital", "name": "Hanoi",              "country": "Viet Nam",                 "lat":  21.0285,  "lon": 105.8542},
    {"type": "capital", "name": "Sanaa",              "country": "Yemen",                    "lat":  15.3694,  "lon":  44.1910},
    {"type": "capital", "name": "Lusaka",             "country": "Zambia",                   "lat": -15.3875,  "lon":  28.3228},
    {"type": "capital", "name": "Harare",             "country": "Zimbabwe",                 "lat": -17.8292,  "lon":  31.0522},

    # ---- Event cities not already listed as capitals above ----
    # Independence Day / Canada Day / Bastille Day / Bonfire Night / Australia Day
    {"type": "city", "name": "Sydney",         "country": "Australia",      "lat": -33.8688, "lon": 151.2093},
    {"type": "city", "name": "Rio de Janeiro", "country": "Brazil",         "lat": -22.9068, "lon": -43.1729},
    {"type": "city", "name": "Dubai",          "country": "UAE",            "lat":  25.2048, "lon":  55.2708},
    {"type": "city", "name": "New York",       "country": "United States",  "lat":  40.7128, "lon": -74.0060},
    {"type": "city", "name": "Los Angeles",    "country": "United States",  "lat":  34.0522, "lon":-118.2437},
    {"type": "city", "name": "Chicago",        "country": "United States",  "lat":  41.8781, "lon": -87.6298},
    {"type": "city", "name": "Toronto",        "country": "Canada",         "lat":  43.6532, "lon": -79.3832},
    {"type": "city", "name": "Vancouver",      "country": "Canada",         "lat":  49.2827, "lon":-123.1207},
    {"type": "city", "name": "Lyon",           "country": "France",         "lat":  45.7640, "lon":   4.8357},
    {"type": "city", "name": "Marseille",      "country": "France",         "lat":  43.2965, "lon":   5.3698},
    {"type": "city", "name": "Birmingham",     "country": "United Kingdom", "lat":  52.4862, "lon":  -1.8904},
    {"type": "city", "name": "Manchester",     "country": "United Kingdom", "lat":  53.4808, "lon":  -2.2426},
    {"type": "city", "name": "Melbourne",      "country": "Australia",      "lat": -37.8136, "lon": 144.9631},
    {"type": "city", "name": "Sao Paulo",      "country": "Brazil",         "lat": -23.5505, "lon": -46.6333},
    {"type": "city", "name": "Valencia",       "country": "Spain",          "lat":  39.4699, "lon":  -0.3763},
    # Rhein in Flammen
    {"type": "city", "name": "Bonn",           "country": "Germany",        "lat":  50.7374, "lon":   7.0982},
    {"type": "city", "name": "Cologne",        "country": "Germany",        "lat":  50.9333, "lon":   6.9500},
    {"type": "city", "name": "St. Goar",       "country": "Germany",        "lat":  50.1533, "lon":   7.7144},
    {"type": "city", "name": "Bingen",         "country": "Germany",        "lat":  49.9667, "lon":   7.8989},
    {"type": "city", "name": "Koblenz",        "country": "Germany",        "lat":  50.3569, "lon":   7.5890},
    # Chinese New Year
    {"type": "city", "name": "Shanghai",       "country": "China",          "lat":  31.2304, "lon": 121.4737},
    {"type": "city", "name": "Hong Kong",      "country": "China",          "lat":  22.3193, "lon": 114.1694},
    {"type": "city", "name": "Guangzhou",      "country": "China",          "lat":  23.1291, "lon": 113.2644},
    {"type": "city", "name": "San Francisco",  "country": "United States",  "lat":  37.7749, "lon":-122.4194},
    # Diwali
    {"type": "city", "name": "Mumbai",         "country": "India",          "lat":  19.0760, "lon":  72.8777},
    {"type": "city", "name": "Jaipur",         "country": "India",          "lat":  26.9124, "lon":  75.7873},
    {"type": "city", "name": "Varanasi",       "country": "India",          "lat":  25.3176, "lon":  82.9739},
    {"type": "city", "name": "Amritsar",       "country": "India",          "lat":  31.6340, "lon":  74.8723},
    {"type": "city", "name": "Colombo",        "country": "Sri Lanka",      "lat":   6.9271, "lon":  79.8612},
    # Loy Krathong
    {"type": "city", "name": "Chiang Mai",     "country": "Thailand",       "lat":  18.7883, "lon":  98.9853},
    {"type": "city", "name": "Sukhothai",      "country": "Thailand",       "lat":  17.0054, "lon":  99.8258},
    {"type": "city", "name": "Phuket",         "country": "Thailand",       "lat":   7.8804, "lon":  98.3923},
    {"type": "city", "name": "Yangon",         "country": "Myanmar",        "lat":  16.8661, "lon":  96.1951},
    # Eid al-Adha
    {"type": "city", "name": "Mecca",          "country": "Saudi Arabia",   "lat":  21.3891, "lon":  39.8579},
    {"type": "city", "name": "Istanbul",       "country": "Türkiye",        "lat":  41.0082, "lon":  28.9784},
    {"type": "city", "name": "Casablanca",     "country": "Morocco",        "lat":  33.5731, "lon":  -7.5898},
    {"type": "city", "name": "Karachi",        "country": "Pakistan",       "lat":  24.8607, "lon":  67.0011},
    # Nagaoka / Katakai
    {"type": "city", "name": "Nagaoka",        "country": "Japan",          "lat":  37.4469, "lon": 138.8509},
    {"type": "city", "name": "Niigata",        "country": "Japan",          "lat":  37.9162, "lon": 139.0364},
    {"type": "city", "name": "Katakai",        "country": "Japan",          "lat":  37.5000, "lon": 138.9833},
    # Malta
    {"type": "city", "name": "Marsaxlokk",     "country": "Malta",          "lat":  35.8417, "lon":  14.5431},
]

# ---------------- API HELPER ----------------

def fetch_elevation_batch(locations_batch):
    """
    Fetch elevation for a batch of locations using Open-Meteo Elevation API.
    Accepts up to 100 locations per request.
    """
    lats = ",".join(str(loc["lat"]) for loc in locations_batch)
    lons = ",".join(str(loc["lon"]) for loc in locations_batch)

    url = f"{ELEVATION_URL}?latitude={lats}&longitude={lons}"

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.json().get("elevation", [])
        except Exception as e:
            logging.warning(f"Retry {attempt+1}/{MAX_RETRIES}: {e}")
            time.sleep(5)
    return [None] * len(locations_batch)

# ---------------- MAIN ----------------

BATCH_SIZE = 100

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()

    for i in range(0, len(ALL_LOCATIONS), BATCH_SIZE):
        batch = ALL_LOCATIONS[i:i + BATCH_SIZE]
        logging.info(f"Fetching elevation batch {i//BATCH_SIZE + 1} ({len(batch)} locations)")

        elevations = fetch_elevation_batch(batch)

        for loc, elev in zip(batch, elevations):
            writer.writerow({
                "location_type": loc["type"],
                "name":          loc["name"],
                "country":       loc["country"],
                "latitude":      loc["lat"],
                "longitude":     loc["lon"],
                "elevation_m":   elev,
            })

        time.sleep(REQUEST_DELAY)

logging.info(f"Elevation data saved to {OUTPUT_FILE}")
logging.info(f"Total locations processed: {len(ALL_LOCATIONS)}")
