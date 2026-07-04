
# NYC Taxi Analytics — PySpark Pipeline

End-to-end data engineering pipeline processing 5.4M NYC taxi trips through 
a medallion architecture using PySpark, with a Streamlit analytics dashboard.

## Architecture

## Tech Stack

| Layer | Tool |
|---|---|
| Processing | PySpark 3.5 |
| Storage | Parquet (partitioned by month) |
| Infrastructure | Docker |
| Dashboard | Streamlit + Plotly |

## Pipeline

| Layer | Rows | Description |
|---|---|---|
| Bronze | 5,972,131 | Raw data + ingestion metadata, partitioned by month |
| Silver | 5,423,120 | Cleaned, enriched with derived columns (duration, speed, time of day) |
| Gold | 5 tables | Hourly, daily, zone, payment, weekday aggregations |

## Data Quality (Silver Layer)
- Dropped 549,011 rows (9.2%) — nulls, invalid fares, impossible speeds
- Added derived columns: trip duration, speed mph, time of day, tip percentage
- Filtered to valid 2024 timestamps only

## Gold Tables
- `hourly_metrics` — revenue + volume by hour of day
- `daily_metrics` — daily revenue trend, weekday vs weekend
- `zone_metrics` — top pickup zones by revenue
- `payment_metrics` — breakdown by payment type
- `weekday_metrics` — day of week patterns

## Setup

```bash
git clone https://github.com/yourusername/nyc-taxi-spark.git
cd nyc-taxi-spark

# Start Jupyter container
docker compose up -d

# Download NYC TLC data to data/raw/
# https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

# Run pipeline
docker compose exec jupyter /usr/local/spark/bin/spark-submit /home/jovyan/src/bronze.py
docker compose exec jupyter /usr/local/spark/bin/spark-submit /home/jovyan/src/silver.py
docker compose exec jupyter /usr/local/spark/bin/spark-submit /home/jovyan/src/gold.py

# Run dashboard
python -m streamlit run dashboard/app.py
```

## Dashboard
- 5 KPI cards (trips, revenue, avg fare, distance, tip %)
- Daily revenue trend with weekend highlighting
- Hourly trip volume by time of day
- Day of week pattern
- Payment type breakdown
- Top 15 pickup zones by revenue
