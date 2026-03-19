"""
build_database.py
==================
Read all CSV-Folder and create SQLite-Database + Parquet exports.

Tables:
  1. silvester_airquality_weather_2013_2025   (raw_data/)
  2. silvester_soil_2013_2025                 (raw_data_soil/)
  3. fixed_events_2013_2025                   (raw_data_fixed_events/)
  4. variable_events_2013_2025                (raw_data_variable_events/)
  5. reference_days_2013_2025                 (raw_data_reference/)
  6. extended_cities_2013_2025                (raw_data_extended_cities/)
  7. city_population_2013_2025                (city_population_2013_2025.csv)

Normalizations:
  Silvester (raw_data / raw_data_soil):
    - "capital"    → "city"
    - "year_label" → "year" (int, first year from "2013_2014")
    - w_soil_temperature_0cm, w_soil_moisture_0_1cm → dropped (100% NaN)

  Country names (all tables):
    - Uniform ISO/UN-like notation
    - Known deviations are normalized (see COUNTRY_MAP)
"""

import pandas as pd
import sqlite3
import time
from pathlib import Path

# Minimum row counts per table – adjust if expected sizes change
MIN_ROWS = {
    "silvester_airquality_weather_2013_2025": 1_000,
    "silvester_soil_2013_2025":               1_000,
    "fixed_events_2013_2025":                 500,
    "variable_events_2013_2025":              500,
    "reference_days_2013_2025":               500,
    "extended_cities_2013_2025":              5_000,
    "city_population_2013_2025":              50,
}

PARQUET_DIR = Path("parquet_export")
PARQUET_DIR.mkdir(exist_ok=True)


# ─── Country Normalization ────────────────────────────────────────────────────

COUNTRY_MAP = {
    # Notation in CSVs                        → Target format
    "Czechia":                                "Czechia",
    "DR Congo":                               "DR Congo",
    "Viet Nam":                               "Vietnam",
    "Türkiye":                                "Türkiye",
    "Lao PDR":                                "Lao PDR",
    "Republic of Korea":                      "Republic of Korea",
    "Russian Federation":                     "Russian Federation",
    # Possible variants from external sources
    "Czech Republic":                         "Czechia",
    "Democratic Republic of Congo":           "DR Congo",
    "Democratic Republic of the Congo":       "DR Congo",
    "Vietnam":                                "Vietnam",
    "Turkey":                                 "Türkiye",
    "Laos":                                   "Lao PDR",
    "South Korea":                            "Republic of Korea",
    "Korea, Republic of":                     "Republic of Korea",
    "Russia":                                 "Russian Federation",
}


def normalize_countries(df: pd.DataFrame) -> pd.DataFrame:
    if "country" in df.columns:
        df["country"] = df["country"].map(
            lambda x: COUNTRY_MAP.get(x, x) if pd.notna(x) else x
        )
    return df


# ─── Silvester Normalization ──────────────────────────────────────────────────

def normalize_silvester(df: pd.DataFrame) -> pd.DataFrame:
    if "capital" in df.columns:
        df = df.rename(columns={"capital": "city"})
    if "year_label" in df.columns:
        df["year"] = df["year_label"].str.split("_").str[0].astype(int)
        df = df.drop(columns=["year_label"])
    drop_cols = ["w_soil_temperature_0cm", "w_soil_moisture_0_1cm"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    return normalize_countries(df)


# ─── Helper: read and merge CSVs ─────────────────────────────────────────────

def read_csvs(folder: str, normalize_fn=None) -> pd.DataFrame:
    paths = list(Path(folder).glob("*.csv"))
    if not paths:
        raise FileNotFoundError(f"No CSVs found in '{folder}'.")
    dfs = [pd.read_csv(f) for f in paths]
    df = pd.concat(dfs, ignore_index=True)
    if normalize_fn:
        df = normalize_fn(df)
    else:
        df = normalize_countries(df)
    return df


# ─── Helper: validate row count ──────────────────────────────────────────────

def validate(df: pd.DataFrame, table: str) -> None:
    minimum = MIN_ROWS.get(table, 1)
    if len(df) < minimum:
        raise ValueError(
            f"Table '{table}' has only {len(df):,} rows – "
            f"expected at least {minimum:,}. Aborting before write."
        )


# ─── Helper: create indices ───────────────────────────────────────────────────

def create_indices(conn, table: str, columns: list) -> None:
    for col in columns:
        safe = col.replace("_", "")[:12]
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table[:14]}_{safe} "
            f"ON {table}({col})"
        )


# ─── Helper: write table + parquet ───────────────────────────────────────────

def write_table(df: pd.DataFrame, table: str, conn, index_cols: list) -> float:
    validate(df, table)
    t0 = time.time()
    df.to_sql(table, conn, if_exists="replace", index=False)
    create_indices(conn, table, index_cols)
    df.to_parquet(PARQUET_DIR / f"{table}.parquet", index=False)
    elapsed = time.time() - t0
    print(f"  {table:<48} {len(df):>8,} rows   {elapsed:5.1f}s")
    return elapsed


# ─── Main ─────────────────────────────────────────────────────────────────────

DB_PATH = "fireworks_events_2013_2025.db"
conn = sqlite3.connect(DB_PATH)

print(f"Building '{DB_PATH}' ...\n")
print(f"  {'Table':<48} {'Rows':>8}   {'Time':>5}")
print(f"  {'-'*67}")

total_start = time.time()

# 1. Silvester – Weather + Air Quality
df = read_csvs("raw_data", normalize_fn=normalize_silvester)
write_table(df, "silvester_airquality_weather_2013_2025", conn,
            ["country", "city", "year", "time"])

# 2. Silvester – Soil
df = read_csvs("raw_data_soil", normalize_fn=normalize_silvester)
write_table(df, "silvester_soil_2013_2025", conn,
            ["country", "city", "year", "time"])

# 3. Fixed Events
df = read_csvs("raw_data_fixed_events")
write_table(df, "fixed_events_2013_2025", conn,
            ["event", "city", "country", "year", "time"])

# 4. Variable Events
df = read_csvs("raw_data_variable_events")
write_table(df, "variable_events_2013_2025", conn,
            ["event", "city", "country", "year", "time"])

# 5. Reference Days
df = read_csvs("raw_data_reference")
write_table(df, "reference_days_2013_2025", conn,
            ["event", "city", "country", "year", "time", "reference_type"])

# 6. Extended Cities
df = read_csvs("raw_data_extended_cities")
write_table(df, "extended_cities_2013_2025", conn,
            ["category", "city", "country", "year", "time"])

# 7. City Population (Lookup)
df = pd.read_csv("city_population_2013_2025.csv")
df = normalize_countries(df)
write_table(df, "city_population_2013_2025", conn,
            ["city", "country", "year"])

conn.commit()
conn.close()

total = time.time() - total_start
print(f"\n  Total: {total:.1f}s")
print(f"\nDatabase '{DB_PATH}' successfully built.")
print(f"Parquet files written to '{PARQUET_DIR}/'.")