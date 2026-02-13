# ✈️ Aviation DWH — US Flight Analytics

> A data warehouse for analyzing flight cancellations and delays across US airports,  
> built as part of a data engineering study project by a team of 4.

---

## What is this?

We took real US flight data and asked a simple question:  
**why do flights get cancelled — and can we predict when it's likely to happen?**

Turns out, weather is responsible for 67–79% of all cancellations.  
This project builds the infrastructure to show that: a full 4-layer data warehouse,  
8 Airflow pipelines, and a live dashboard in Yandex DataLens.

---

## What's inside

```
aviation-dwh/
├── sql/
│   └── dm_layer_queries.sql     # all 4 analytical mart queries
└── docs/
    ├── architecture.md          # full layer-by-layer technical breakdown
    └── report.md                # analytical findings and dashboard walkthrough
```

---

## The stack

| Tool | What we used it for |
|---|---|
| **PostgreSQL** | Storage for all 4 DWH layers |
| **Apache Airflow** | 8 DAGs orchestrating the full ETL pipeline |
| **Yandex DataLens** | BI dashboard — 7 charts, live data |
| **S3** | Source file storage for raw flight data |
| **METAR archive** | Hourly weather observations per airport |

---

## How the data flows

```
[BTS flights]    [OurAirports]    [METAR weather]
      │                │                 │
      ▼                ▼                 ▼
   ods.*          ods.airport        ods.weather
      │                                  │
      ▼──────────────────────────────────▼
   stg.flights                    stg.weather
   (clean, deduplicate,           (normalize, extract
    cast timestamps)               ICAO codes)
      │                                  │
      ▼──────────────────────────────────▼
   dds.success_flights            dds.weather  ← SCD2
   dds.cancel_flights             dds.airport
      │
      ▼
   dm.success_flights        dm.dep_arr_report
   dm.cancel_flights         dm.cancellation_report
      │
      ▼
   Yandex DataLens dashboard
```

---

## Key findings

After wiring everything together and running the dashboard, here's what the data actually showed:

- **Weather causes 67–79% of all cancellations** across all 4 airports (code B)
- **January is the worst month** — peak cancellations, especially at AVL
- **Most delays are 5–7 minutes**, but the tail reaches 150 min for outliers
- **Snow at BIS drops average temperature to −7°C** — strong predictor of cancellations
- **Hub routes dominate** — AVL sends most flights to LGA, ORD, DFW

---

## The pipeline in detail

9 Airflow DAGs, all tagged `team11test`, running in sequence:

**ODS layer** — load raw data from sources
- `Azimova_flights_ods_11` — parses flight files from S3
- `Azimova_k_airport_ods_11` — loads airport reference
- `Azimova_weather_ods_11` — processes METAR archives by ICAO code

**STG layer** — clean and normalize
- `azimovak_flights_stg_11` — casts timestamps, deduplicates by `(flight, date, origin, dest)`
- `azimova_weather_stg_11` — normalizes weather fields, removes duplicates

**DDS layer** — build the warehouse
- `azimova_airport_dds_11` — adds surrogate keys to airport dimension
- `azimova_flights_dds_11` — splits flights into success/cancel fact tables
- `azimova_weather_dds_11` — builds SCD2 history for weather changes

**DM layer** — create analytical marts
- `krasovskayas_dm_flights_full_etl` — single DAG that creates all 4 DM tables

---

## The dashboard

7 charts in Yandex DataLens, all connected to DM and DDS layers:

| # | Chart | What it answers |
|---|---|---|
| 1 | Airport map | Where are the busiest airports? What's their cancellation rate? |
| 2 | Flight distance by destination | Which airports serve long-haul routes? |
| 3 | Cancellations by month | When should you worry about scheduling? |
| 4 | Cancellation reasons by airport | What's the dominant risk at each airport? |
| 5 | Route distribution (tree-map) | Which directions carry the most traffic? |
| 6 | Delay histograms | How bad are delays at each airport, really? |
| 7 | Temperature & snow table | What weather conditions precede cancellations? |

---

## Data sources

| Source | What it provides |
|---|---|
| [BTS — Bureau of Transportation Statistics](https://www.transtats.bts.gov) | US flight records: times, delays, cancellation codes |
| [OurAirports](https://ourairports.com) | Airport reference: IATA/ICAO codes, coordinates, city, region |
| METAR archive | Hourly weather: temperature, pressure, wind speed, visibility, cloud cover |

---

## Airports in scope

| Code | City | State |
|---|---|---|
| AVL | Asheville | North Carolina |
| FAY | Fayetteville | North Carolina |
| BIS | Bismarck | North Dakota |
| GFK | Grand Forks | North Dakota |

---

## Team

This project was built by a team of 4 as part of a data engineering course (Group 11):

- **Карина Азимова** — ETL pipelines (ODS → STG → DDS), Airflow DAGs
- **Альбина Бахтиарова** — data modeling, DDS layer design
- **Софья Красовская** — DM layer, SQL mart queries
- **Диана Мкоян** — time series analysis, DataLens dashboard

---

## Want to explore the SQL?

All DM-layer queries are in [`sql/dm_layer_queries.sql`](sql/dm_layer_queries.sql) —  
fully commented, with explanations of why each join and transformation works the way it does.

For the full architecture breakdown: [`docs/architecture.md`](docs/architecture.md)  
For analytical findings: [`docs/report.md`](docs/report.md)
