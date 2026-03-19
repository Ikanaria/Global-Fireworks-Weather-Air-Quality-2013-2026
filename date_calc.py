"""
date_calc.py
====================
Calculates and verifies all fireworks event dates 2013-2025.
Variable dates (lunar calendar) use verified lookup tables.
Fixed dates are computed automatically from the year.
Run this script first to verify all dates before data collection.
"""

from datetime import date, timedelta
import json

# ============================================================
# VARIABLE DATES - verified against official sources
# ============================================================

# Chinese New Year (Spring Festival) - first day of lunar new year
# Source: timeanddate.com, chinesenewyear.net
CHINESE_NEW_YEAR = {
    2013: date(2013,  2, 10),
    2014: date(2014,  1, 31),
    2015: date(2015,  2, 19),
    2016: date(2016,  2,  8),
    2017: date(2017,  1, 28),
    2018: date(2018,  2, 16),
    2019: date(2019,  2,  5),
    2020: date(2020,  1, 25),
    2021: date(2021,  2, 12),
    2022: date(2022,  2,  1),
    2023: date(2023,  1, 22),
    2024: date(2024,  2, 10),
    2025: date(2025,  1, 29),
}

# Diwali (Festival of Lights) - 15th day of Kartik, Hindu lunar calendar
# Source: timeanddate.com, diwalifestival.org
DIWALI = {
    2013: date(2013, 11,  3),
    2014: date(2014, 10, 23),
    2015: date(2015, 11, 11),
    2016: date(2016, 10, 30),
    2017: date(2017, 10, 19),
    2018: date(2018, 11,  7),
    2019: date(2019, 10, 27),
    2020: date(2020, 11, 14),
    2021: date(2021, 11,  4),
    2022: date(2022, 10, 24),
    2023: date(2023, 11, 12),
    2024: date(2024, 11,  1),
    2025: date(2025, 10, 20),
}

# Loy Krathong (Thai Festival of Lights) - full moon of 12th Thai lunar month
# Source: timeanddate.com, travelthru.com
LOY_KRATHONG = {
    2013: date(2013, 11, 17),
    2014: date(2014, 11,  6),
    2015: date(2015, 11, 25),
    2016: date(2016, 11, 14),
    2017: date(2017, 11,  3),
    2018: date(2018, 11, 22),
    2019: date(2019, 11, 11),
    2020: date(2020, 10, 31),
    2021: date(2021, 11, 19),
    2022: date(2022, 11,  8),
    2023: date(2023, 11, 27),
    2024: date(2024, 11, 15),
    2025: date(2025, 11,  5),
}

# Eid al-Adha - 10th day of Dhu al-Hijja, Islamic lunar calendar
# Source: timeanddate.com, islamicfinder.org
EID_AL_ADHA = {
    2013: date(2013, 10, 15),
    2014: date(2014, 10,  5),
    2015: date(2015,  9, 24),
    2016: date(2016,  9, 12),
    2017: date(2017,  9,  1),
    2018: date(2018,  8, 22),
    2019: date(2019,  8, 11),
    2020: date(2020,  7, 31),
    2021: date(2021,  7, 20),
    2022: date(2022,  7,  9),
    2023: date(2023,  6, 28),
    2024: date(2024,  6, 16),
    2025: date(2025,  6,  6),
}

# Bonfire Night / Guy Fawkes - always November 5th, but if Sunday shifted
# In practice always celebrated Nov 5 weekend - we use Nov 5 directly
# Japanese Nagaoka Fireworks Festival - first Saturday of August
def nagaoka_date(year):
    """First Saturday of August - main Nagaoka fireworks night."""
    d = date(year, 8, 1)
    # weekday(): Monday=0, Saturday=5
    days_until_sat = (5 - d.weekday()) % 7
    return d + timedelta(days=days_until_sat)

# Japanese Katakai Festival - last Saturday of September
def katakai_date(year):
    """Last Saturday of September."""
    d = date(year, 9, 30)
    days_back = (d.weekday() - 5) % 7
    return d - timedelta(days=days_back)

# Malta International Fireworks Festival - last Saturday of April
def malta_fireworks_date(year):
    """Last Saturday of April."""
    d = date(year, 4, 30)
    days_back = (d.weekday() - 5) % 7
    return d - timedelta(days=days_back)

# ============================================================
# FIXED DATES - computed automatically
# ============================================================

def get_fixed_dates(year):
    """Return all fixed-date fireworks events for a given year."""
    return {
        "new_years_eve":     date(year, 12, 31),   # Global
        "independence_day":  date(year,  7,  4),   # USA
        "canada_day":        date(year,  7,  1),   # Canada
        "bastille_day":      date(year,  7, 14),   # France
        "bonfire_night":     date(year, 11,  5),   # UK, Ireland
        "australia_day":     date(year,  1, 26),   # Australia
        "singapore_natday":  date(year,  8,  9),   # Singapore
        "brazil_indep":      date(year,  9,  7),   # Brazil
        "las_fallas":        date(year,  3, 19),   # Spain (Valencia)
        "el_salvador_fire":  date(year,  8, 31),   # El Salvador
        "rhein_flammen_1":   date(year,  5,  6),   # Germany (Bonn, approx.)
        "rhein_flammen_2":   date(year,  7, 15),   # Germany (St. Goar, approx.)
        "rhein_flammen_3":   date(year,  9, 16),   # Germany (Koblenz, approx.)
    }

def get_variable_dates(year):
    """Return all variable-date fireworks events for a given year."""
    return {
        "chinese_new_year":  CHINESE_NEW_YEAR[year],
        "diwali":            DIWALI[year],
        "loy_krathong":      LOY_KRATHONG[year],
        "eid_al_adha":       EID_AL_ADHA[year],
        "nagaoka":           nagaoka_date(year),
        "katakai":           katakai_date(year),
        "malta_fireworks":   malta_fireworks_date(year),
    }

# ============================================================
# VERIFICATION & EXPORT
# ============================================================

if __name__ == "__main__":
    all_events = {}

    print("=" * 60)
    print("ALL FIREWORKS EVENT DATES 2013-2025")
    print("=" * 60)

    for year in range(2013, 2026):
        fixed    = get_fixed_dates(year)
        variable = get_variable_dates(year)
        all_events[year] = {**fixed, **variable}

        print(f"\n--- {year} ---")
        for event, d in sorted({**fixed, **variable}.items(), key=lambda x: x[1]):
            print(f"  {event:<25} {d.strftime('%Y-%m-%d')} ({d.strftime('%A')})")

    # Export to JSON for use in collector scripts
    export = {
        str(year): {
            event: d.strftime("%Y-%m-%d")
            for event, d in events.items()
        }
        for year, events in all_events.items()
    }

    with open("event_dates.json", "w") as f:
        json.dump(export, f, indent=2)

    print("\n" + "=" * 60)
    print("event_dates.json saved - use this in collector scripts.")
    print("=" * 60)