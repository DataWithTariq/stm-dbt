# 🚍 STM Real-Time Transit Data Platform

A production-grade data engineering platform analyzing **1.6M+ GPS positions** from Montreal's STM bus network to generate **fleet optimization recommendations** — helping transit planners add buses where riders need them most and reduce waste on underused routes.

Built on **Databricks** with **Delta Lake** and **dbt**, following modern data engineering best practices: medallion architecture, star schema modeling, additive measures pattern, automated data quality, and full orchestration.

---

## The Business Problem

Every winter in Montreal, thousands of riders watch overcrowded buses pass their stop — meanwhile, other routes run nearly empty. The STM operates **211 bus routes** but lacks granular, data-driven insights to dynamically reallocate fleet capacity.

**This platform answers:**
- Which routes are consistently overcrowded and need more buses?
- How does weather (especially snow) shift rider demand across the network?
- Where can the STM reallocate buses from underused routes to high-demand corridors?
- What does the hourly demand profile look like — are we staffing peaks correctly?

---

## Key Business Insights

### Fleet Optimization
Analysis of 211 routes over 14 days revealed a **16.3% network-wide overcrowding rate**, with significant variation across routes:

- **15 routes need additional buses** — overcrowded 30%+ of the time (e.g. Sherbrooke corridor)
- **30 routes are candidates for reduction** — underused 90%+ of the time
- **Net opportunity**: Reallocating just 10% of capacity from empty routes to overcrowded corridors could serve thousands more riders daily with zero additional cost
  
<img width="1818" height="1094" alt="image" src="https://github.com/user-attachments/assets/f65a21af-41d3-4dd4-b013-fe54ce726d42" />

### Weather Impact on Demand
Snow events increase overcrowding by **+5 to +15 percentage points** on major corridors — riders who normally walk or cycle switch to transit. Routes like Sherbrooke (105, 24, 185) absorb the most weather-driven demand.

**Actionable recommendation**: Pre-position extra buses on high-sensitivity corridors during snow forecasts, and verify shelter infrastructure at high-demand stops.

### Peak Hour Analysis
- **AM Peak (6-9h)** and **PM Peak (15-18h)** show the highest overcrowding, but some routes experience demand spikes during off-peak hours — suggesting schedule review rather than fleet additions
- Weekday vs Weekend patterns differ significantly: some routes flip from overcrowded to empty on weekends

### Corridor-Level Analysis
Multiple routes share the same corridor (e.g. Sherbrooke = routes 105 + 24 + 185). By aggregating at the corridor level using additive measures, the platform reveals whether overcrowding is a route-specific issue or a corridor-wide capacity problem — a distinction that changes the operational response.

---

## Architecture

```
┌─────────────────────┐     ┌──────────────┐     ┌──────────────────────────────────┐
│    DATA SOURCES      │     │   BRONZE     │     │     SILVER & GOLD (dbt)          │
│                      │     │  (PySpark)   │     │                                  │
│  STM GTFS-RT API ────┼────►│ vehicle_pos  │     │  10 Staging Views                │
│  (Protobuf, 5 min)   │     │              ├────►│  7 Silver Tables (dims + facts)  │
│                      │     │              │     │  5 Gold Tables (analytics)       │
│  STM Static GTFS ────┼────►│ gtfs_*       │     │                                  │
│  (Monthly refresh)   │     │              │     │  104 Automated Data Tests        │
│                      │     │              │     │                                  │
│  Open-Meteo API ─────┼────►│ weather      │     │         ┌──────────────┐         │
│  (Daily backfill)    │     │              │     │         │  Power BI    │         │
│                      │     │ quarantine   │     │         │  5 Pages     │         │
└──────────────────────┘     └──────────────┘     └─────────┴──────────────┴─────────┘
```

> **Scope**: 211 bus routes. Metro excluded — subway vehicles use internal signaling, not GPS via GTFS-RT.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Cloud Platform** | Databricks (Unity Catalog) |
| **Storage** | Delta Lake (Lakehouse) |
| **Ingestion** | PySpark, Protobuf, REST APIs |
| **Transformation** | dbt-core + dbt-databricks |
| **Orchestration** | Databricks Workflows (4 jobs) |
| **Data Quality** | dbt tests (104 assertions) + quarantine table |
| **Version Control** | Git + GitHub |
| **BI Layer** | Power BI (Import mode, DAX measures) |

---

## Data Sources

| Source | Format | Frequency | Volume |
|--------|--------|-----------|--------|
| STM GTFS-RT | Protobuf | Every 5 min | ~160K records/day |
| STM Static GTFS | CSV (zipped) | Monthly | 250+ routes, 8,900+ stops, 78K+ trips, 2.6M+ stop times |
| Open-Meteo | JSON REST API | Daily backfill | 24 hourly observations/day |

---

## Medallion Architecture

### Bronze Layer (PySpark Notebooks)
Raw data parsed and stored as Delta tables with full metadata tracking.

| Table | Source | Records |
|-------|--------|---------|
| `vehicle_positions` | GTFS-RT Protobuf | ~160K/day |
| `gtfs_routes` | Static GTFS | 250+ routes |
| `gtfs_stops` | Static GTFS | 8,900+ stops |
| `gtfs_trips` | Static GTFS | 78,000+ trips |
| `gtfs_stop_times` | Static GTFS | 2.6M+ stop times |
| `gtfs_calendar` | Static GTFS | Service schedules |
| `weather` | Open-Meteo API | 24 records/day |
| `quarantine` | Error handling | Failed records with blame trail |

### Silver Layer (dbt — 17 models)

**Staging Views (10)** — Type casting, cleaning, UTC→Montreal timezone conversion:

`stg_vehicle_positions` · `stg_weather` · `stg_routes` · `stg_stops` · `stg_trips` · `stg_stop_times` · `stg_calendar` · `stg_calendar_dates` · `stg_shapes` · `stg_agency`

**Dimension & Fact Tables (7)** — Star schema:

| Model | Type | Description |
|-------|------|-------------|
| `dim_routes` | Dimension | Route attributes — single source of truth for names, types, colors |
| `dim_stops` | Dimension | Stop locations with GPS coordinates |
| `dim_calendar` | Dimension | Service calendar with pattern classification (Weekday/Weekend/Daily) |
| `dim_date` | Dimension | Date spine with fiscal periods, weekday labels, relative dates |
| `fact_vehicle_positions` | Fact | Deduplicated GPS positions with dimension keys |
| `fact_trips` | Fact | Trips enriched with route context and direction labels |
| `fact_stop_times` | Fact | Stop times denormalized with stop/trip/route context |

### Gold Layer (dbt — 5 models)

All Gold fact tables follow the **additive measures pattern**: only raw counts and sums are stored. Ratios, percentages, and recommendations are computed dynamically in Power BI DAX — ensuring mathematical correctness at any aggregation level (route, corridor, or network).

| Model | Grain | Purpose |
|-------|-------|---------|
| `fct_daily_performance` | route × day | Daily route metrics + weather context |
| `fct_fleet_optimization` | route × day | Fleet optimization: overcrowding counts, peak hour breakdowns, speed by period |
| `fct_occupancy_by_hour` | route × day × hour | Hourly occupancy for heatmap drill-down and peak analysis |
| `fct_route_analytics` | route | GTFS schedule statistics: trip counts, stop coverage, direction balance |
| `obt_positions_wide` | position | One Big Table — every GPS position with route + hourly weather. Designed as a semantic layer for ad-hoc BI queries and AI/ML workloads |

---

## Star Schema Design

```
                    dim_date
                   /    |    \
   fct_daily_perf  fct_fleet  fct_occupancy
                \    |    /
                dim_routes
               /         \
   fct_route_analytics   obt_positions_wide
```

**Design principles:**
- **FK-only facts** — Route names live in `dim_routes` only, not duplicated across fact tables
- **Additive measures** — Facts store counts and sums (`overcrowded_count`, `sum_speed`), never pre-calculated percentages
- **Degenerate dimensions** — Low-cardinality attributes like `dominant_weather` and `day_type` stay in facts

---

## Power BI Dashboard

5 interactive pages with dynamic DAX measures computed from additive base metrics:

| Page | Focus |
|------|-------|
| **Fleet Overview** | Network-wide KPIs: total positions, active buses, peak hours, weekday/weekend split |
| **Weather Impact Analysis** | Snow vs Clear overcrowding, weather sensitivity by route, corridor-level impact |
| **Route Analysis** | GTFS schedule insights: trip counts, stop coverage, direction balance, top routes |
| **Service Time Patterns** | Hourly demand heatmap, time band analysis, AM/PM peak patterns |
| **Fleet Optimization** | Actionable recommendations: Add Buses / Monitor / Reduce / Schedule Review |

**Dynamic DAX measures** — All recommendations and KPIs are computed live from additive counts, not stored as text columns. Example:
```dax
-- Fleet Recommendation recalculates correctly at any aggregation level
Fleet Recommendation =
VAR OPct = DIVIDE(
    CALCULATE(SUM([overcrowded_count]), ALL(fct_fleet_optimization), ...),
    CALCULATE(SUM([total_positions]), ALL(fct_fleet_optimization), ...))
RETURN SWITCH(TRUE(),
    OPct >= 0.30, "Add Buses",
    OPct >= 0.20, "Monitor", ...)
```

This pattern ensures that filtering by corridor (e.g. "Sherbrooke") recalculates the recommendation across all routes sharing that corridor — something impossible with pre-computed labels.

📸 **Screenshots**: [docs/PowerBi Dashboard/](docs/PowerBi%20Dashboard/)

---

## Data Quality

**104 automated dbt tests** covering all 22 models across 3 layers:

| Test Type | Count | Purpose |
|-----------|-------|---------|
| `not_null` | 63 | No missing values in critical columns |
| `unique` | 13 | No duplicate primary keys |
| `accepted_values` | 9 | Validated enums: weather categories, service patterns, day types, time periods |
| `relationships` | 8 | Foreign key integrity — every fact FK exists in its dimension |

**Additional quality patterns:**
- **Quarantine table** with blame trail — failed records captured with error reason, source file, and timestamp
- **Orphan route detection** — Relationship tests identified route 568 (temporary service) in GTFS-RT that doesn't exist in the static GTFS feed. Tests set to `severity: warn` to flag without blocking the pipeline
- **HAVING clauses** in Gold models filter out statistical noise (routes with < 10 daily positions excluded)

---

## Pipeline Orchestration

| Job | Schedule | Tasks |
|-----|----------|-------|
| `STM_Vehicle_Positions_Pipeline` | Every 5 minutes | Ingest → Bronze |
| `STM_Weather` | Daily 7:00 AM | Ingest → Bronze → dbt build (22 models + 104 tests) |
| `STM_GTFS_Static_Pipeline` | 1st of month | Ingest → Bronze |
| `STM_Weekly_Maintenance` | Sunday 3:00 AM | OPTIMIZE + VACUUM Delta tables |

---

## Project Structure

```
stm-dbt/
├── models/
│   ├── staging/              # 10 staging views + configs
│   │   ├── _stg__sources.yml
│   │   ├── _stg__models.yml      # 104 test definitions
│   │   ├── stg_vehicle_positions.sql
│   │   ├── stg_weather.sql
│   │   └── ...
│   ├── silver/               # 7 dimension & fact tables
│   │   ├── dim_routes.sql
│   │   ├── dim_stops.sql
│   │   ├── dim_calendar.sql
│   │   ├── dim_date.sql
│   │   ├── fact_vehicle_positions.sql
│   │   ├── fact_trips.sql
│   │   └── fact_stop_times.sql
│   └── gold/                 # 5 analytics tables
│       ├── fct_daily_performance.sql
│       ├── fct_fleet_optimization.sql
│       ├── fct_occupancy_by_hour.sql
│       ├── fct_route_analytics.sql
│       └── obt_positions_wide.sql
├── macros/
│   └── custom_schema.sql
├── docs/
│   └── PowerBi Dashboard/
├── databricks_notebooks/
│   ├── 00_setup/
│   ├── 01_ingestion/
│   └── 02_bronze/
├── dbt_project.yml
├── packages.yml
└── README.md
```

---

## Key Engineering Decisions

| Decision | Why |
|----------|-----|
| **Additive measures in Gold** | Pre-calculated percentages break when aggregated across routes or corridors. Raw counts ensure `SUM(overcrowded) / SUM(total)` is correct at any level |
| **Star schema with FK-only facts** | Route names live in `dim_routes` only — no duplication across 5 fact tables |
| **Dynamic DAX recommendations** | Fleet recommendations computed live from counts, enabling correct recalculation when filtering by corridor, weather, or time period |
| **OBT for AI/ML readiness** | `obt_positions_wide` provides a fully denormalized semantic layer for pandas, Spark ML, or LLM analytics without complex joins |
| **Protobuf parsing in PySpark** | GTFS-RT binary decoding at ingestion, not deferred downstream |
| **UTC → Montreal timezone** | Converted at staging layer — all downstream models work in local time |
| **Speed conversion (m/s → km/h)** | Applied in staging (`speed * 3.6`) for business-readable values |
| **Quarantine with blame trail** | Failed records captured with error context — never silently dropped |
| **Relationship tests with severity warn** | Orphan routes flagged but don't block the pipeline |
| **GTFS time as STRING** | Stop times kept as STRING — GTFS allows values > 24:00:00 for overnight trips |

---

## Learning Resources

This project was built applying principles from:

- **Fundamentals of Data Engineering** (Joe Reis & Matt Housley) — Medallion architecture, idempotency, data quality as code, the 6 Undercurrents framework
- **Designing Data-Intensive Applications** (Martin Kleppmann) — Schema-on-write, incremental processing, fault tolerance patterns
- **Storytelling with Data** (Cole Nussbaumer Knaflic) — Dashboard design: clear visual hierarchy, actionable insights over raw numbers, narrative text explaining the "so what"

---

## Getting Started

### Prerequisites
- Databricks workspace with Unity Catalog
- Python 3.11+
- dbt-core + dbt-databricks

### Setup
```bash
git clone https://github.com/DataWithTariq/stm-dbt.git
cd stm-dbt

pip install dbt-core dbt-databricks
dbt deps
dbt build   # run 22 models + 104 tests
```

---

## Author

**Tariq** — Data Engineer building lakehouse architectures on Databricks and Microsoft Fabric.

[LinkedIn](https://www.linkedin.com/in/tariq-ladidji-b08951311/) · [GitHub](https://github.com/DataWithTariq)
