"""
Automated ETL Data Pipeline
============================
Extract → Transform → Load pipeline using two free public APIs:

Free APIs used:
  1. Open-Meteo Weather API   https://api.open-meteo.com
     100% FREE | No API key | No account | No credit card
     Fetches hourly temperature + wind speed for multiple cities.

  2. REST Countries API       https://restcountries.com/v3.1/
     100% FREE | No API key | No account | No credit card
     Fetches country metadata (population, area, languages, timezone).

Data is transformed, joined, and loaded into a local SQLite database.
Full logging, error handling, and a summary report are included.
"""

import os, json, sqlite3, logging, time
from datetime import datetime, timedelta
from typing import Optional
import requests
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ─── LOGGING ──────────────────────────────────────────────────────────────────
os.makedirs("outputs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("outputs/etl_pipeline.log", mode="w"),
    ],
)
logger = logging.getLogger(__name__)

DB_PATH = "data/etl_database.db"


# ─── EXTRACT LAYER ────────────────────────────────────────────────────────────

CITIES = {
    "London":     {"lat": 51.51, "lon": -0.13, "country_code": "GB"},
    "New York":   {"lat": 40.71, "lon": -74.01, "country_code": "US"},
    "Tokyo":      {"lat": 35.69, "lon": 139.69, "country_code": "JP"},
    "Dubai":      {"lat": 25.20, "lon": 55.27,  "country_code": "AE"},
    "Lahore":     {"lat": 31.55, "lon": 74.34,  "country_code": "PK"},
    "Sydney":     {"lat": -33.87, "lon": 151.21, "country_code": "AU"},
}


def extract_weather(city: str, lat: float, lon: float,
                    days_back: int = 3) -> Optional[pd.DataFrame]:
    """
    Extract weather data from Open-Meteo API.
    FREE — https://api.open-meteo.com | No key | No account
    Returns hourly temp (°C) and windspeed (km/h) for past N days.
    """
    end_date   = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        f"&hourly=temperature_2m,windspeed_10m"
        f"&start_date={start_date}&end_date={end_date}"
        f"&timezone=auto"
    )
    try:
        logger.info(f"[EXTRACT] Weather → {city} ({lat}, {lon})")
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        d = r.json()
        df = pd.DataFrame({
            "city":         city,
            "timestamp":    pd.to_datetime(d["hourly"]["time"]),
            "temperature_c": d["hourly"]["temperature_2m"],
            "windspeed_kmh": d["hourly"]["windspeed_10m"],
        })
        logger.info(f"[EXTRACT] ✓ {city}: {len(df)} hourly records")
        return df
    except Exception as e:
        logger.error(f"[EXTRACT] Weather failed for {city}: {e}")
        return None


def extract_country_info(country_code: str) -> Optional[dict]:
    """
    Extract country metadata from REST Countries API.
    FREE — https://restcountries.com | No key | No account
    """
    url = f"https://restcountries.com/v3.1/alpha/{country_code}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        d = r.json()[0]
        languages = list(d.get("languages", {}).values())
        currencies = list(d.get("currencies", {}).keys())
        return {
            "country_code":  country_code,
            "country_name":  d.get("name", {}).get("common", ""),
            "region":        d.get("region", ""),
            "subregion":     d.get("subregion", ""),
            "population":    d.get("population", 0),
            "area_km2":      d.get("area", 0),
            "languages":     ", ".join(languages[:3]),
            "currencies":    ", ".join(currencies[:2]),
            "timezone":      (d.get("timezones") or ["UTC"])[0],
        }
    except Exception as e:
        logger.error(f"[EXTRACT] Country info failed ({country_code}): {e}")
        return None


def extract_all() -> tuple:
    """Run all extract jobs and return raw dataframes."""
    logger.info("=" * 55)
    logger.info("EXTRACT PHASE — Fetching from free APIs")
    logger.info("=" * 55)

    weather_frames = []
    for city, coords in CITIES.items():
        df = extract_weather(city, coords["lat"], coords["lon"])
        if df is not None:
            weather_frames.append(df)
        time.sleep(0.3)  # polite rate limiting

    weather_df = pd.concat(weather_frames, ignore_index=True) if weather_frames else pd.DataFrame()
    logger.info(f"[EXTRACT] Total weather records: {len(weather_df)}")

    country_records = []
    seen = set()
    for city, coords in CITIES.items():
        cc = coords["country_code"]
        if cc not in seen:
            info = extract_country_info(cc)
            if info:
                country_records.append(info)
                seen.add(cc)
            time.sleep(0.2)

    country_df = pd.DataFrame(country_records)
    logger.info(f"[EXTRACT] Country records fetched: {len(country_df)}")
    return weather_df, country_df


# ─── TRANSFORM LAYER ──────────────────────────────────────────────────────────

def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Clean, validate, and enrich weather data."""
    logger.info("\nTRANSFORM PHASE — Cleaning and enriching data")

    original_len = len(df)

    # Drop nulls
    df = df.dropna(subset=["temperature_c", "windspeed_kmh"])

    # Validate ranges
    df = df[df["temperature_c"].between(-60, 60)]
    df = df[df["windspeed_kmh"].between(0, 300)]

    removed = original_len - len(df)
    if removed:
        logger.warning(f"[TRANSFORM] Removed {removed} invalid weather records")

    # Feature engineering
    df["temp_fahrenheit"]  = (df["temperature_c"] * 9/5) + 32
    df["temp_category"]    = pd.cut(df["temperature_c"],
                                     bins=[-60,-10,0,15,25,60],
                                     labels=["Freezing","Cold","Cool","Warm","Hot"])
    df["wind_category"]    = pd.cut(df["windspeed_kmh"],
                                     bins=[-1,5,20,50,150,300],
                                     labels=["Calm","Breeze","Moderate","Strong","Storm"])
    df["hour"]             = df["timestamp"].dt.hour
    df["date"]             = df["timestamp"].dt.date.astype(str)
    df["is_daytime"]       = df["hour"].between(6, 20).astype(int)
    df["extracted_at"]     = datetime.now().isoformat()

    logger.info(f"[TRANSFORM] ✓ Weather: {len(df)} clean records, "
                f"{df.shape[1]} features")
    return df


def transform_country(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich country data."""
    df = df.copy()
    df["population_millions"] = (df["population"] / 1_000_000).round(2)
    df["pop_density_per_km2"] = (df["population"] / df["area_km2"].replace(0, np.nan)).round(2)
    df["extracted_at"]        = datetime.now().isoformat()
    logger.info(f"[TRANSFORM] ✓ Countries: {len(df)} records enriched")
    return df


def transform_join(weather_df: pd.DataFrame,
                   country_df: pd.DataFrame) -> pd.DataFrame:
    """Join weather and country data on city→country mapping."""
    city_to_code = {city: coords["country_code"] for city, coords in CITIES.items()}
    weather_df["country_code"] = weather_df["city"].map(city_to_code)
    joined = weather_df.merge(country_df[["country_code","country_name","region",
                                           "population_millions","timezone"]],
                               on="country_code", how="left")
    logger.info(f"[TRANSFORM] ✓ Joined dataset: {joined.shape}")
    return joined


# ─── LOAD LAYER ───────────────────────────────────────────────────────────────

def load_to_sqlite(weather_df: pd.DataFrame,
                   country_df: pd.DataFrame,
                   joined_df: pd.DataFrame):
    """Load all transformed tables into SQLite database."""
    logger.info(f"\nLOAD PHASE — Writing to SQLite: {DB_PATH}")

    con = sqlite3.connect(DB_PATH)

    weather_df.to_sql("weather_hourly",  con, if_exists="replace", index=False)
    country_df.to_sql("country_metadata", con, if_exists="replace", index=False)
    joined_df.to_sql("weather_enriched",  con, if_exists="replace", index=False)

    # Verify
    cursor = con.cursor()
    for table in ["weather_hourly", "country_metadata", "weather_enriched"]:
        count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info(f"[LOAD] ✓ Table '{table}': {count} rows")

    con.close()
    logger.info(f"[LOAD] ✓ Database written → {DB_PATH}")


# ─── ANALYSE & VISUALISE ──────────────────────────────────────────────────────

def analyse_and_plot(weather_df: pd.DataFrame, country_df: pd.DataFrame):
    logger.info("\nANALYSIS PHASE — Generating plots")

    city_stats = (weather_df.groupby("city")["temperature_c"]
                  .agg(["mean","min","max"]).round(2).reset_index())
    city_stats.columns = ["city","avg_temp","min_temp","max_temp"]

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle("ETL Pipeline — Weather Analysis Across Cities",
                 fontsize=14, fontweight="bold")

    # Avg temperature by city
    axes[0].barh(city_stats["city"], city_stats["avg_temp"],
                 color="#2196F3", edgecolor="white")
    axes[0].set_title("Average Temperature (°C)")
    axes[0].set_xlabel("Temperature (°C)")
    axes[0].axvline(0, color="red", linestyle="--", linewidth=1)
    axes[0].grid(axis="x", alpha=.3)

    # Temperature range
    axes[1].barh(city_stats["city"],
                 city_stats["max_temp"] - city_stats["min_temp"],
                 color="#4CAF50", edgecolor="white")
    axes[1].set_title("Temperature Range (°C)")
    axes[1].set_xlabel("Max - Min Temp (°C)")
    axes[1].grid(axis="x", alpha=.3)

    # Hourly temp trend for one city
    sample_city = weather_df[weather_df["city"] == "London"].copy()
    if len(sample_city) > 0:
        sample_city = sample_city.sort_values("timestamp").tail(48)
        axes[2].plot(range(len(sample_city)), sample_city["temperature_c"],
                     color="#FF5722", linewidth=2)
        axes[2].fill_between(range(len(sample_city)),
                              sample_city["temperature_c"], alpha=.2, color="#FF5722")
        axes[2].set_title("London — Last 48h Temperature Trend")
        axes[2].set_xlabel("Hours (last 48)"); axes[2].set_ylabel("Temp (°C)")
        axes[2].grid(alpha=.3)

    plt.tight_layout()
    plt.savefig("outputs/etl_analysis.png", dpi=150)
    plt.close()
    logger.info("[PLOT] etl_analysis.png saved")

    city_stats.to_csv("outputs/city_temperature_stats.csv", index=False)
    logger.info("[OUTPUT] city_temperature_stats.csv saved")
    return city_stats


# ─── PIPELINE REPORT ──────────────────────────────────────────────────────────

def save_pipeline_report(weather_df, country_df, city_stats):
    report = {
        "pipeline_run":    datetime.now().isoformat(),
        "apis_used": [
            {"name": "Open-Meteo Weather API", "url": "https://api.open-meteo.com",
             "key_required": False, "cost": "Free"},
            {"name": "REST Countries API",     "url": "https://restcountries.com",
             "key_required": False, "cost": "Free"},
        ],
        "cities_processed":  list(CITIES.keys()),
        "weather_records":   len(weather_df),
        "country_records":   len(country_df),
        "database":          DB_PATH,
        "city_temperature_summary": city_stats.to_dict(orient="records"),
    }
    with open("outputs/pipeline_report.json", "w") as f:
        json.dump(report, f, indent=2)
    logger.info("[REPORT] pipeline_report.json saved")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("AUTOMATED ETL PIPELINE — START")
    start = time.time()

    # EXTRACT
    weather_raw, country_raw = extract_all()

    # TRANSFORM
    weather_clean  = transform_weather(weather_raw)
    country_clean  = transform_country(country_raw)
    joined_df      = transform_join(weather_clean, country_clean)

    # LOAD
    load_to_sqlite(weather_clean, country_clean, joined_df)

    # ANALYSE
    city_stats = analyse_and_plot(weather_clean, country_clean)

    # REPORT
    save_pipeline_report(weather_clean, country_clean, city_stats)

    elapsed = round(time.time() - start, 2)
    logger.info(f"\nPIPELINE COMPLETE in {elapsed}s | Outputs → outputs/ | DB → {DB_PATH}")
