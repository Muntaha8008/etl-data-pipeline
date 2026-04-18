# ⚙️ Automated ETL Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)
![SQLite](https://img.shields.io/badge/SQLite-Database-lightgrey?logo=sqlite)
![API](https://img.shields.io/badge/Open--Meteo%20API-Free-green)
![API](https://img.shields.io/badge/REST%20Countries%20API-Free-green)

> A production-style **Extract → Transform → Load** pipeline that ingests live weather data and country metadata from two free public APIs, transforms and enriches the data, and loads it into a SQLite database — with full logging and automated reports.

---

## 📌 Project Highlights

| Feature | Detail |
|---|---|
| **Architecture** | Classic ETL with distinct Extract / Transform / Load layers |
| **APIs** | Open-Meteo + REST Countries — both 100% free, no key |
| **Storage** | SQLite (3 normalised tables) |
| **Cities** | London, New York, Tokyo, Dubai, Lahore, Sydney |
| **Logging** | Full structured logs to console + file |

---

## 🌐 API Integrations — Both Completely Free

### 1. Open-Meteo Weather API
```
https://api.open-meteo.com/v1/forecast?latitude=...&longitude=...&hourly=temperature_2m
```
- Historical + forecast hourly weather
- **Cost:** Free | **Key:** None required | **Docs:** https://open-meteo.com/en/docs

### 2. REST Countries API
```
https://restcountries.com/v3.1/alpha/GB
```
- Country population, region, languages, area, timezone
- **Cost:** Free | **Key:** None required | **Docs:** https://restcountries.com

> ⚠️ **Security:** This project uses no API keys. The `.gitignore` excludes `.env` files regardless.

---

## 🏗️ Pipeline Architecture

```
┌─────────────────────────────────────────────────────┐
│                    EXTRACT                          │
│  Open-Meteo API ──→ Hourly weather (6 cities)       │
│  REST Countries ──→ Country metadata                │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                   TRANSFORM                         │
│  • Null removal & range validation                  │
│  • Feature engineering (temp categories, etc.)     │
│  • JOIN weather + country on country_code           │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                     LOAD                            │
│  SQLite DB → weather_hourly                         │
│           → country_metadata                        │
│           → weather_enriched (joined)               │
└─────────────────────────────────────────────────────┘
```

---

## 🚀 Quickstart

```bash
git clone https://github.com/YOUR_USERNAME/etl-data-pipeline
cd etl-data-pipeline

pip install -r requirements.txt
python etl_pipeline.py
```

**Outputs:**
```
outputs/
├── etl_pipeline.log           # Full execution log
├── etl_analysis.png           # Temperature charts
├── city_temperature_stats.csv
└── pipeline_report.json

data/
└── etl_database.db            # SQLite with 3 tables
```

---

## 📋 SQLite Tables

```sql
SELECT * FROM weather_hourly;      -- Hourly weather per city
SELECT * FROM country_metadata;    -- Country info
SELECT * FROM weather_enriched;    -- Joined + enriched
```

---

## 📁 Project Structure

```
etl-data-pipeline/
├── etl_pipeline.py    # Full ETL pipeline
├── requirements.txt
├── .gitignore
├── .env.example
├── outputs/
└── data/
```

---

## 🧰 Tech Stack

`pandas` · `requests` · `SQLite3` · `Open-Meteo API` · `REST Countries API` · `logging`

---

## 👤 Author

**Muntaha** — Data Engineer | ML Engineer  
[LinkedIn](#) · [GitHub](#)
