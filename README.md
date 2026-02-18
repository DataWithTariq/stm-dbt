# 🚍 STM Real-Time Transit Data Platform

A production-grade data engineering platform analyzing ** daily 160k+ GPS positions** from Montreal's STM bus network to generate **fleet optimization recommendations** — helping transit planners add buses where riders need them most and reduce waste on underused routes.

Built on **Databricks** with **Delta Lake** and **dbt**, following modern data engineering best practices: medallion architecture, star schema modeling, additive measures pattern, automated data quality, and full orchestration.

---

## The Business Problem

Every winter in Montreal, thousands of riders watch overcrowded buses pass their stop — meanwhile, other routes run nearly empty. The STM operates **215 bus routes** but lacks granular, data-driven insights to dynamically reallocate fleet capacity.

**This platform answers:**
- Which routes are consistently overcrowded and need more buses?
- How does weather (especially snow) shift rider demand across the network?
- Where can the STM reallocate buses from underused routes to high-demand corridors?
- What does the hourly demand profile look like — are we staffing peaks correctly?

---

## Key Business Insights

### Fleet Optimization
Analysis of 211 routes over 18 days revealed a **16.0% network-wide overcrowding rate**, with significant variation across routes:


- **15 routes need additional buses** — overcrowded 30%+ of the time (Sherbrooke at 37%, Jean-Talon Est at 36%, Viau at 35%)
- **30 routes are candidates for reduction** — underused 90%+ of the time
- **100 routes are operating optimally** (47.4% of the network)
- **Net opportunity**: Reallocating capacity from the 30 underused routes to the 15 overcrowded corridors could serve thousands more riders daily with zero additional cost

### Fleet Imbalance — The Mirror View
By comparing the top overcrowded routes against the top underused routes (filtered to major routes by network volume share), the platform reveals a clear reallocation path:
- **Overcrowded side**: Sherbrooke (37%), Jean-Talon (36%), Viau (35%) — buses running at standing room or full
- **Underused side**: Saint-Charles (84%), Pointe-aux-Trembles (81%), Monk (69%) — buses running near-empty
- The actionable insight: move buses from the right column to the left column
- **15 routes need additional buses** — overcrowded 30%+ of the time (e.g. Sherbrooke corridor)
- **30 routes are candidates for reduction** — underused 90%+ of the time
- **Net opportunity**: Reallocating just 10% of capacity from empty routes to overcrowded corridors could serve thousands more riders daily with zero additional cost
  
<img width="1818" height="1094" alt="image" src="https://github.com/user-attachments/assets/f65a21af-41d3-4dd4-b013-fe54ce726d42" />
>>>>>>> c178755a28f5b3225f446b50aa95f0f47a0ed491

### Weather Impact on Demand
Using `snow_hours` (hours of snowfall per day) instead of dominant weather category provides more accurate analysis. Days with **4+ hours of snow** show measurably higher overcrowding compared to snow-free days.

Example: Express Sherbrooke sees **47.0% overcrowding on snow days (4h+)** vs **39.4% on non-snow days** — a +7.6 percentage point difference. Riders who normally walk or cycle switch to transit during sustained snowfall.

**Actionable recommendation**: Pre-position extra buses on high-sensitivity corridors during snow forecasts, and verify shelter infrastructure at high-demand stops.

### Peak Hour Analysis
- **Peak hour at 16:00** (4 PM) with the highest bus deployment across the network
- **75.9% of all service volume** occurs on weekdays
- **PM Peak (3-6 PM)** shows the highest total volume, followed by AM Peak (6-9 AM)
- Service drops sharply after 9 PM — some routes flip from overcrowded during peaks to near-empty off-peak, suggesting schedule review rather than fleet additions
- The Day × Hour heatmap reveals that Thursday and Friday afternoons carry the heaviest load

### Corridor-Level Analysis
Multiple routes share the same corridor (e.g. Sherbrooke = routes 105 + 24 + 185). By aggregating at the corridor level using additive measures, the platform reveals whether overcrowding is a route-specific issue or a corridor-wide capacity problem — a distinction that changes the operational response.

### Service Frequency vs Coverage
A scatter plot analysis of trips-per-stop reveals three distinct route categories:
- **High Frequency (green)**: 30+ trips per stop — major corridors like YUL Aéroport (747), Côte-des-Neiges (165)
- **Medium Frequency (yellow)**: 10-30 trips per stop — standard urban routes
- **Low Frequency (red)**: <10 trips per stop — high infrastructure cost for minimal service, candidates for restructuring

---

## Architecture

```
┌──────────────────────┐     ┌──────────────┐     ┌──────────────────────────────────┐
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

![STM Architecture](docs/stm_architecture.png)
```

> **Scope**: 215 bus routes. Metro excluded — subway vehicles use internal signaling, not GPS via GTFS-RT.

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
| `fct_daily_performance` | route × day | Daily route metrics + weather context (temperature, precipitation, snow_hours) |
| `fct_fleet_optimization` | route × day | Fleet optimization: overcrowding counts, peak hour breakdowns, speed by period, snow_hours |
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
- **Degenerate dimensions** — Low-cardinality attributes like `dominant_weather`, `day_type`, and `snow_hours` stay in facts

---

## Power BI Dashboard

5 interactive pages with dynamic DAX measures computed from additive base metrics:

### Fleet Overview
Network-wide operational snapshot: GPS coverage map of Montreal, fleet imbalance mirror chart (overcrowded vs underused top routes by volume), average active buses by hour showing dual AM/PM peaks, and KPIs (2.47M positions, 1,630 unique buses, 215 routes, 14 km/h avg speed).

### Weather Impact Analysis
Weather KPIs (avg temperature, feels-like, humidity, wind), daily service volume vs temperature trend, weather sensitivity comparison showing overcrowding delta between snow days (4h+) and non-snow days by route, dynamic narrative text with actionable recommendations, and a Weather × Weekday matrix.

### Route Analysis
Service Frequency vs Coverage scatter plot with 3-color categorization (High/Medium/Low frequency based on trips-per-stop ratio), route network treemap showing planned capacity by corridor with individual route breakdowns, and a corridor detail table with trip counts, stops, and direction balance.

### Service Time Patterns
Positions by time band, average service volume by day of week, hourly volume curve (0-23h), and a Day × Hour heatmap revealing that Thursday/Friday PM peaks carry the heaviest network load.

### Fleet Optimization
Top priority overcrowded routes with conditional color coding (Add Buses vs Monitor), fleet recommendation breakdown across 5 categories (Optimal/Monitor/Schedule Review/Reduce/Add), optimization summary narrative, and an Overcrowding Heatmap (Route × Hour) with conditional formatting showing exactly when and where overcrowding peaks.

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
| **snow_hours instead of dominant_weather** | A day with 3h of snow categorized as "Cloudy" by dominant weather hides the snow impact. `snow_hours` counts actual hours of snowfall, letting the BI layer define the threshold (4h+) |
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
- **The Data Warehouse Toolkit** (Ralph Kimball & Margy Ross) — Star schema design, dimensional modeling, additive vs non-additive measures, conformed dimensions
- **Designing Data-Intensive Applications** (Martin Kleppmann) — Schema-on-write, incremental processing, fault tolerance patterns
- **Storytelling with Data** (Cole Nussbaumer Knaflic) — Dashboard design: clear visual hierarchy, actionable insights over raw numbers, conditional formatting to guide interpretation

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

**Tariq** — Aspiring Data Engineer based in Montreal. This project was built to explore real-time transit data and apply modern data engineering practices end-to-end.

[LinkedIn](https://www.linkedin.com/in/tariq-ladidji-b08951311/) · [GitHub](https://github.com/DataWithTariq)
