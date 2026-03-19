# Global Fireworks & Air Quality Dataset (2013–2026)

A comprehensive environmental dataset linking **fireworks events worldwide** to
hourly weather, air quality, and soil data — collected to study the measurable
impact of pyrotechnics on atmospheric conditions.

---

## Dataset at a Glance

| Metric | Value |
|--------|-------|
| Total rows | 191,931 |
| Tables | 9 |
| Years covered | 2013–2026 |
| Countries | 155 capitals + 153 additional cities |
| Cities | 300+ |
| Data collection time | 37 hours API runtime across 5 days |
| Data sources | Open-Meteo (Weather + Air Quality), NASA POWER (Soil), UN/World Bank (Population) |

---

## Database Schema

The dataset is provided as a **SQLite database** (`fireworks_events_2013_2025.db`)
and as individual **Parquet files** (`parquet_export/`).

### Tables Overview

| Table | Rows | Description |
|-------|------|-------------|
| `silvester_airquality_weather_2013_2025` | 20,280 | New Year's Eve weather & air quality for world capitals |
| `silvester_soil_2013_2025` | 20,280 | Soil data for the same capitals |
| `fixed_events_2013_2025` | 4,458 | Fixed-date annual fireworks events |
| `variable_events_2013_2025` | 9,282 | Variable-date events (Diwali, Eid, CNY, etc.) |
| `reference_days_2013_2025` | 15,210 | Baseline days −14/−7/+7 around each event |
| `extended_cities_2013_2025` | 119,325 | 153 global cities across 11 event categories |
| `city_population_2013_2025` | 871 | City-level population estimates per year |
| `country_population_2013_2025` | 2,028 | Country-level population, density, urbanization |
| `elevation_all_locations` | 197 | Elevation in meters for all locations |

### Column Prefixes

| Prefix | Domain | Source |
|--------|--------|--------|
| `w_` | Weather (temperature, wind, precipitation, radiation, …) | Open-Meteo Historical Weather API |
| `a_` | Air quality (PM10, PM2.5, NO₂, SO₂, O₃, CO, dust, …) | Open-Meteo Air Quality API |
| `s_` | Soil (moisture top/root/profile, temperature) | NASA POWER API |

---

## Events Covered

### New Year's Eve (Silvester)
Global coverage: **world capitals** across all continents, 2013–2026.
5 measurement hours per night: 00:00, 01:00, 02:00, 22:00, 23:00.

### Extended City Coverage (119,325 rows)

| Category | Cities | Countries | Description |
|----------|--------|-----------|-------------|
| `megacity` | 100 | 52 | World's largest urban agglomerations |
| `large_country` | 26 | 7 | Multiple cities per large country (Russia, China, USA, etc.) |
| `event_eid` | 5 | 5 | Eid al-Fitr & Eid al-Adha (Baghdad, Tripoli, Amman, Mogadishu, Kabul) |
| `event_cny` | 5 | 5 | Chinese New Year (Taipei, Medan, Bangkok, Sydney, London) |
| `event_diwali` | 3 | 3 | Diwali (Ayodhya, Leicester, Suva) |
| `event_bonfire` | 4 | 2 | Bonfire Night (Leeds, Bristol, Auckland, Wellington) |
| `event_malta` | 2 | 1 | Malta International Fireworks Festival (Mdina, Gozo) |
| `event_loy` | 2 | 1 | Loi Krathong (Lampang, Hat Yai) |
| `event_july4` | 2 | 1 | Independence Day USA (San Diego, Nashville) |
| `event_japan_fw` | 2 | 1 | Japanese Summer Fireworks (Kyoto, Omagari) |
| `event_silvester` | 2 | 2 | Special Silvester cities (Edinburgh, Rome) |

### Reference Days
Each event includes **3 baseline measurement days**:
- `ref_minus14` — 14 days before the event
- `ref_minus7` — 7 days before the event
- `ref_plus7` — 7 days after the event

This enables direct before/after comparison to isolate fireworks impact.

---

## Joining Tables

All main tables share `country`, `city`, `year`, and `time` columns.

```sql
-- Example: PM10 on New Year's Eve vs. baseline, Berlin
SELECT
    s.year,
    s.data_type,
    AVG(s.a_pm10) AS avg_pm10
FROM extended_cities_2013_2025 s
WHERE s.city = 'Berlin'
GROUP BY s.year, s.data_type
ORDER BY s.year, s.data_type;

-- Example: Join with population
SELECT
    e.city, e.country, e.year,
    e.a_pm10, e.w_temperature_2m,
    p.population_estimate
FROM silvester_airquality_weather_2013_2025 e
LEFT JOIN city_population_2013_2025 p
    ON e.city = p.city AND e.country = p.country AND e.year = p.year
WHERE e.year = 2023
ORDER BY e.city;
```

---

## File Structure

```
fireworks_events_2013_2025.db   ← SQLite database (all 9 tables)
parquet_export/
  ├── silvester_airquality_weather_2013_2025.parquet
  ├── silvester_soil_2013_2025.parquet
  ├── fixed_events_2013_2025.parquet
  ├── variable_events_2013_2025.parquet
  ├── reference_days_2013_2025.parquet
  ├── extended_cities_2013_2025.parquet
  ├── city_population_2013_2025.parquet
  ├── country_population_2013_2025.parquet
  └── elevation_all_locations.parquet
```

---

## Key Findings & Research Questions

Analysis of this dataset revealed several unexpected results:

- **SO₂ and NO₂ fall on New Year's Eve** (−52% and −35% vs. Ref −7), because holiday
  traffic reductions outweigh pyrotechnic emissions — making PM10 a more reliable
  fireworks indicator than gas-phase measurements.
- **Drizzle nearly halves PM10** compared to dry nights (15.1 vs. 28.9 μg/m³),
  and the effect is continuous, not a threshold.
- **City size and urbanization do not predict fireworks air quality.** Weather does —
  particularly boundary layer height and wind speed.
- **Diwali produces the strongest air quality impact** of all celebrations covered.
- The fireworks PM10 signal in Berlin, London, and Paris **has not weakened over 13 years**
  despite ongoing regulatory discussions.

Open questions for further exploration:
- How does the boundary layer height interact with precipitation to determine PM10 peaks?
- Are there measurable differences in fireworks impact between Eid celebrations across
  Muslim-majority cities?
- How does the Japanese summer fireworks season compare to winter events when controlling
  for season?
- Does the Malta International Fireworks Festival — one of Europe's most intense —
  show a detectable signal despite the island's coastal winds?

---

## Technical Notes

- **Timezone**: All timestamps are in **local time** of the respective city.
- **Measurement hours**: 00:00, 01:00, 02:00, 22:00, 23:00 local time.
- **Missing values**: Some air quality variables (e.g. `a_carbon_monoxide`) may be
  `NaN` for remote locations where Open-Meteo has no model coverage.
- **Country names**: Normalized throughout (e.g. `Viet Nam` → `Vietnam`,
  `Viet Nam` → `Vietnam`, `Türkiye` consistent).
- **ERA5 reanalysis**: Weather data uses ERA5 reanalysis for past dates,
  ERA5-Land for soil variables.

---

## About

Dataset compiled and curated by **Franziska Tannert**.  
Data collection: February 2026 | API runtime: ~37 hours | Collection period: 5 days

*Data sources: [Open-Meteo](https://open-meteo.com/) (free tier, CC BY 4.0),
[NASA POWER](https://power.larc.nasa.gov/) (public domain),
UN World Urbanization Prospects, World Bank.*
