# 🚍 STM Real-Time Transit Data Platform

A production-grade data engineering platform that ingests, transforms, and models Montreal's public transit (STM) data in real-time, combining vehicle positions, GTFS schedules, and weather data into analytics-ready tables.

Built on **Databricks** with **Delta Lake** and **dbt**, following modern data engineering best practices: medallion architecture, incremental processing, automated data quality, and full orchestration.

## Architecture

```
┌─────────────────────┐     ┌──────────────┐    ┌──────────────────────────────┐
│     DATA SOURCES    │     │    BRONZE    │    │   SILVER & GOLD (dbt)        │
│                     │     │  (PySpark)   │    │                              │
│  STM GTFS-RT API ───┼────►│ vehicle_pos  │    │  10 Staging Views            │
│  (Protobuf, 5 min   │     │              ├───►│  6 Silver Tables (dims/facts)│
│                     │     │              │    │  3 Gold Tables (analytics)   │
│  STM Static GTF ────┼────►│ gtfs_*       │    │                              │
│  (Monthly refrsh)   │     │              │    │  57 Automated Data Tests     │
│                     │     │              │    │                              │
│  Open-Meteo PI ─────┼────►│ weather      │    │                              │
│  (Daily bacfill)    │     │              │    │                              │
└─────────────────────┘      └────────────┘     └──────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Cloud Platform** | Databricks (Unity Catalog) |
| **Storage** | Delta Lake (Lakehouse) |
| **Ingestion** | PySpark, Protobuf, REST APIs |
| **Transformation** | dbt-core + dbt-databricks |
| **Orchestration** | Databricks Workflows |
| **Data Quality** | dbt tests (57 assertions) |
| **Version Control** | Git + GitHub |
| **BI Layer** | Power BI (Gold tables) |

## Data Sources

- **STM GTFS-RT** — Real-time vehicle positions via Protobuf API (every 5 minutes, ~160K records/day)
- **STM Static GTFS** — Routes, stops, trips, schedules, shapes, calendar (monthly refresh)
- **Open-Meteo** — Hourly weather observations for Montreal (temperature, precipitation, wind, conditions)

## Medallion Architecture

### Bronze (PySpark Notebooks)
Raw data parsed and stored as Delta tables with full metadata tracking.

| Table | Source | Records |
|-------|--------|---------|
| `vehicle_positions` | GTFS-RT Protobuf | ~160K/day |
| `gtfs_routes` | Static GTFS | 250+ routes |
| `gtfs_stops` | Static GTFS | 8,900+ stops |
| `gtfs_trips` | Static GTFS | 78,000+ trips |
| `gtfs_stop_times` | Static GTFS | 2.6M+ stop times |
| `weather` | Open-Meteo API | 24 records/day |

### Silver (dbt — 16 models)

**Staging Views (10)** — Type casting, cleaning, business logic:
`stg_vehicle_positions` · `stg_weather` · `stg_routes` · `stg_stops` · `stg_trips` · `stg_stop_times` · `stg_calendar` · `stg_calendar_dates` · `stg_shapes` · `stg_agency`

**Dimension & Fact Tables (6)**:
- `dim_routes` — Route dimension with type descriptions and surrogate keys
- `dim_stops` — Stop dimension with GPS coordinates
- `dim_calendar` — Service calendar with pattern classification (Weekday/Weekend/Daily)
- `fact_vehicle_positions` — Deduplicated positions with dimension keys
- `fact_trips` — Trips enriched with route context and direction labels
- `fact_stop_times` — Stop times denormalized with stop/trip/route info

### Gold (dbt — 3 models)

| Model | Purpose |
|-------|---------|
| `fct_daily_performance` | Daily route metrics + weather context (temperature, precipitation, dominant weather) |
| `fct_route_analytics` | Route-level statistics: trip counts, stop coverage, outbound/inbound balance |
| `obt_positions_wide` | One Big Table — every position with route + hourly weather joined (BI-ready) |

## Data Quality

57 automated dbt tests covering all layers:

- **not_null** — No missing values in critical columns
- **unique** — No duplicate primary keys
- **accepted_values** — Validated enums (weather categories, service patterns, exception types)
- **referential integrity** — Foreign key relationships validated

## Pipeline Orchestration

| Job | Schedule | Tasks |
|-----|----------|-------|
| `STM_Vehicle_Positions_Pipeline` | Every 5 minutes | Ingest → Bronze |
| `STM_Weather` | Daily 7:00 AM | Ingest → Bronze → dbt build (19 models + 57 tests) |
| `STM_GTFS_Static_Pipeline` | 1st of month | Ingest → Bronze |
| `STM_Weekly_Maintenance` | Sunday 3:00 AM | OPTIMIZE Delta tables |

## Project Structure

```
stm-dbt/
├── models/
│   ├── staging/          # 10 staging views + source/model configs
│   │   ├── _stg__sources.yml
│   │   ├── _stg__models.yml
│   │   ├── stg_vehicle_positions.sql
│   │   ├── stg_weather.sql
│   │   ├── stg_routes.sql
│   │   └── ...
│   ├── silver/           # 6 dimension & fact tables
│   │   ├── dim_routes.sql
│   │   ├── dim_stops.sql
│   │   ├── dim_calendar.sql
│   │   ├── fact_vehicle_positions.sql
│   │   ├── fact_trips.sql
│   │   └── fact_stop_times.sql
│   └── gold/             # 3 analytics tables
│       ├── fct_daily_performance.sql
│       ├── fct_route_analytics.sql
│       └── obt_positions_wide.sql
├── macros/
│   └── custom_schema.sql
├── dbt_project.yml
├── packages.yml
└── README.md
```

## Key Engineering Decisions

- **Typed Bronze tables** — Columns cast at ingestion (not all-string), reducing Silver complexity
- **Protobuf parsing** — Vehicle positions decoded from binary GTFS-RT format in PySpark
- **Weather × Positions join** — Matched on `date + hour` for hourly weather context in Gold
- **Deduplication** — `ROW_NUMBER()` window function in `fact_vehicle_positions` handles duplicate readings
- **GTFS time handling** — Stop times kept as STRING (values can exceed `24:00:00` for overnight trips)
- **Custom schema macro** — Prevents dbt's default `{prefix}_{schema}` concatenation in Unity Catalog

## Data Engineering Principles Applied

Based on *Fundamentals of Data Engineering* (Reis & Housley):

- **Medallion Architecture** — Bronze (raw) → Silver (cleaned) → Gold (business-ready)
- **Idempotency** — Re-running any pipeline produces the same result
- **Data Quality** — 57 automated tests, schema enforcement, NOT NULL constraints
- **Orchestration** — Dependency-aware scheduling with failure handling
- **Incremental Processing** — Watermark-based ingestion for vehicle positions
- **FinOps** — Serverless SQL warehouse, OPTIMIZE for storage efficiency

## Getting Started

### Prerequisites
- Databricks workspace with Unity Catalog
- Python 3.11+
- dbt-core + dbt-databricks

### Setup
```bash
# Clone the repo
git clone https://github.com/DataWithTariq/stm-dbt.git
cd stm-dbt

# Install dbt
pip install dbt-core dbt-databricks

# Install packages
dbt deps

# Configure connection (edit ~/.dbt/profiles.yml)
# Run models
dbt build  # run + test
```

## Author

**Tariq** — Data Engineer specializing in lakehouse architectures on Databricks and Microsoft Fabric.

[LinkedIn](https://linkedin.com/in/YOUR_LINKEDIN) · [GitHub](https://github.com/DataWithTariq)
