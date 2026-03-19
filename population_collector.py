"""
population_collector.py
========================
Collects country-level population and population density data
for all countries in the dataset, years 2013-2025.
Source: World Bank API (no API key required)
Indicators used:
  SP.POP.TOTL      - Total population
  EN.POP.DNST      - Population density (people per sq. km of land area)
  SP.URB.TOTL.IN.ZS - Urban population (% of total)
"""

import requests
import json
import time
import logging
import csv
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------- CONFIG ----------------

YEARS = range(2013, 2026)

WORLDBANK_URL = "https://api.worldbank.org/v2/country"

REQUEST_DELAY = 2
MAX_RETRIES   = 3

DATA_DIR = Path("raw_data_population")
DATA_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = DATA_DIR / "country_population_2013_2025.csv"

# World Bank indicators
INDICATORS = {
    "SP.POP.TOTL":       "population_total",
    "EN.POP.DNST":       "population_density_per_km2",
    "SP.URB.TOTL.IN.ZS": "urban_population_pct",
}

# World Bank ISO3 codes mapped to country names used in main dataset
# Required because World Bank uses ISO3 codes, not country names
COUNTRY_ISO3 = {
    "Afghanistan":              "AFG",
    "Albania":                  "ALB",
    "Algeria":                  "DZA",
    "Angola":                   "AGO",
    "Argentina":                "ARG",
    "Armenia":                  "ARM",
    "Australia":                "AUS",
    "Austria":                  "AUT",
    "Azerbaijan":               "AZE",
    "Bahrain":                  "BHR",
    "Bangladesh":               "BGD",
    "Belarus":                  "BLR",
    "Belgium":                  "BEL",
    "Belize":                   "BLZ",
    "Benin":                    "BEN",
    "Bhutan":                   "BTN",
    "Bolivia":                  "BOL",
    "Bosnia and Herzegovina":   "BIH",
    "Botswana":                 "BWA",
    "Brazil":                   "BRA",
    "Bulgaria":                 "BGR",
    "Burkina Faso":             "BFA",
    "Burundi":                  "BDI",
    "Cambodia":                 "KHM",
    "Cameroon":                 "CMR",
    "Canada":                   "CAN",
    "Central African Republic": "CAF",
    "Chad":                     "TCD",
    "Chile":                    "CHL",
    "China":                    "CHN",
    "Colombia":                 "COL",
    "Comoros":                  "COM",
    "Congo":                    "COG",
    "Costa Rica":               "CRI",
    "Croatia":                  "HRV",
    "Cuba":                     "CUB",
    "Cyprus":                   "CYP",
    "Czechia":                  "CZE",
    "Denmark":                  "DNK",
    "DR Congo":                 "COD",
    "Ecuador":                  "ECU",
    "Egypt":                    "EGY",
    "El Salvador":              "SLV",
    "Estonia":                  "EST",
    "Eswatini":                 "SWZ",
    "Ethiopia":                 "ETH",
    "Finland":                  "FIN",
    "France":                   "FRA",
    "Gabon":                    "GAB",
    "Gambia":                   "GMB",
    "Georgia":                  "GEO",
    "Germany":                  "DEU",
    "Ghana":                    "GHA",
    "Greece":                   "GRC",
    "Guatemala":                "GTM",
    "Guinea":                   "GIN",
    "Guyana":                   "GUY",
    "Haiti":                    "HTI",
    "Honduras":                 "HND",
    "Hungary":                  "HUN",
    "Iceland":                  "ISL",
    "India":                    "IND",
    "Indonesia":                "IDN",
    "Iran":                     "IRN",
    "Iraq":                     "IRQ",
    "Ireland":                  "IRL",
    "Israel":                   "ISR",
    "Italy":                    "ITA",
    "Jamaica":                  "JAM",
    "Japan":                    "JPN",
    "Jordan":                   "JOR",
    "Kazakhstan":               "KAZ",
    "Kenya":                    "KEN",
    "Kuwait":                   "KWT",
    "Kyrgyzstan":               "KGZ",
    "Lao PDR":                  "LAO",
    "Latvia":                   "LVA",
    "Lebanon":                  "LBN",
    "Lesotho":                  "LSO",
    "Liberia":                  "LBR",
    "Libya":                    "LBY",
    "Lithuania":                "LTU",
    "Luxembourg":               "LUX",
    "Madagascar":               "MDG",
    "Malawi":                   "MWI",
    "Malaysia":                 "MYS",
    "Maldives":                 "MDV",
    "Mali":                     "MLI",
    "Malta":                    "MLT",
    "Mauritania":               "MRT",
    "Mauritius":                "MUS",
    "Mexico":                   "MEX",
    "Mongolia":                 "MNG",
    "Montenegro":               "MNE",
    "Morocco":                  "MAR",
    "Mozambique":               "MOZ",
    "Myanmar":                  "MMR",
    "Namibia":                  "NAM",
    "Nepal":                    "NPL",
    "Netherlands":              "NLD",
    "New Zealand":              "NZL",
    "Nicaragua":                "NIC",
    "Niger":                    "NER",
    "Nigeria":                  "NGA",
    "North Macedonia":          "MKD",
    "Norway":                   "NOR",
    "Oman":                     "OMN",
    "Pakistan":                 "PAK",
    "Panama":                   "PAN",
    "Paraguay":                 "PRY",
    "Peru":                     "PER",
    "Philippines":              "PHL",
    "Poland":                   "POL",
    "Portugal":                 "PRT",
    "Qatar":                    "QAT",
    "Republic of Korea":        "KOR",
    "Republic of Moldova":      "MDA",
    "Romania":                  "ROU",
    "Russian Federation":       "RUS",
    "Rwanda":                   "RWA",
    "Saudi Arabia":             "SAU",
    "Senegal":                  "SEN",
    "Serbia":                   "SRB",
    "Sierra Leone":             "SLE",
    "Singapore":                "SGP",
    "Slovakia":                 "SVK",
    "Slovenia":                 "SVN",
    "Somalia":                  "SOM",
    "South Africa":             "ZAF",
    "South Sudan":              "SSD",
    "Spain":                    "ESP",
    "Sri Lanka":                "LKA",
    "Sudan":                    "SDN",
    "Suriname":                 "SUR",
    "Sweden":                   "SWE",
    "Syria":                    "SYR",
    "Tajikistan":               "TJK",
    "Tanzania":                 "TZA",
    "Thailand":                 "THA",
    "Togo":                     "TGO",
    "Trinidad and Tobago":      "TTO",
    "Tunisia":                  "TUN",
    "Türkiye":                  "TUR",
    "Turkmenistan":             "TKM",
    "Uganda":                   "UGA",
    "Ukraine":                  "UKR",
    "United Arab Emirates":     "ARE",
    "United Kingdom":           "GBR",
    "United States":            "USA",
    "Uruguay":                  "URY",
    "Uzbekistan":               "UZB",
    "Venezuela":                "VEN",
    "Viet Nam":                 "VNM",
    "Yemen":                    "YEM",
    "Zambia":                   "ZMB",
    "Zimbabwe":                 "ZWE",
}

FIELDNAMES = ["country", "iso3", "year"] + list(INDICATORS.values())

# ---------------- API HELPER ----------------

def fetch_indicator(iso3, indicator_code):
    """
    Fetch all years for one country and one indicator from World Bank API.
    Returns dict: {year: value}
    """
    url = (
        f"{WORLDBANK_URL}/{iso3}/indicator/{indicator_code}"
        f"?format=json&per_page=100&mrv=20"
    )
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            # World Bank returns [metadata, data_array]
            if len(data) < 2 or not data[1]:
                return {}
            return {
                int(entry["date"]): entry["value"]
                for entry in data[1]
                if entry["value"] is not None
            }
        except Exception as e:
            logging.warning(f"Retry {attempt+1}/{MAX_RETRIES} {iso3} {indicator_code}: {e}")
            time.sleep(10)
    return {}

# ---------------- MAIN ----------------

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()

    for country, iso3 in COUNTRY_ISO3.items():
        logging.info(f"Fetching population data: {country} ({iso3})")

        # Fetch all indicators for this country
        indicator_data = {}
        for indicator_code, col_name in INDICATORS.items():
            indicator_data[col_name] = fetch_indicator(iso3, indicator_code)
            time.sleep(REQUEST_DELAY)

        # Write one row per year
        for year in YEARS:
            row = {
                "country": country,
                "iso3":    iso3,
                "year":    year,
            }
            for col_name in INDICATORS.values():
                row[col_name] = indicator_data[col_name].get(year)

            writer.writerow(row)

        logging.info(f"  Done: {country}")

logging.info(f"Population data saved to {OUTPUT_FILE}")
