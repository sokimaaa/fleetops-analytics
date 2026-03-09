# FleetOps Analytics

FleetOps Analytics is a Spark-based analytics platform for taxi trip data. The goal is to identify operational problems, monitor key ride metrics, and provide a strong foundation for future analytical jobs.

## Business Context

FleetOps wants to build an analytics system for taxi trip analysis and operational issue detection. This repository provides the baseline project structure, Spark pipeline foundation, and configuration layout for all subsequent tasks.

## Dataset

The project is designed to process taxi trip datasets that typically include:

- trip identifiers
- pickup and dropoff timestamps
- pickup and dropoff zones
- distance and duration
- fare and payment details
- driver and vehicle attributes

Raw files should be placed under `data/raw/`.

## Pipeline Architecture

The repository follows a medallion-style pipeline design:

- `job/bronze/`: ingestion and raw normalization jobs
- `job/silver/`: cleaned and conformed data transformations
- `job/gold/`: business-level aggregates and analytics-ready marts

Storage layers:

- `data/raw/`: source data
- `data/bronze/`: ingested raw structured data
- `data/silver/`: validated and transformed data
- `data/gold/`: curated business metrics and aggregates

Additional domains:

- `domain/`: domain models and business entities
- `config/`: application and environment configurations

## Local Run Instructions

### Prerequisites

- JDK 17+
- SBT 1.10+

### Build and test

```bash
sbt compile
sbt test
```

### Run bootstrap app

```bash
sbt "runMain com.fleetops.analytics.Main"
```

## Package Namespace

Main package namespace: `com.fleetops.analytics`
