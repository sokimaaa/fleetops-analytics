# FleetOps Analytics

FleetOps Analytics is a batch analytics project built with Scala, Apache Spark, and SBT. It processes NYC yellow taxi trip data through a medallion-style pipeline and produces curated operational datasets for monitoring trip volumes, revenue hotspots, anomalies, and data quality.

The repository already contains:

- a raw yellow taxi sample dataset for January 2025
- a taxi zone lookup used for enrichment
- runnable Spark jobs for bronze, silver, and gold layers
- tests covering schema validation, normalization, enrichment, aggregations, and reporting

## What the project does

The pipeline is designed around a small, explicit set of analytical capabilities:

- ingest raw yellow taxi trip parquet files into a partitioned bronze layer
- validate source records and keep row-level validation results
- normalize the raw taxi schema into a cleaner analytics schema
- enrich trips with pickup and dropoff borough/zone metadata
- aggregate daily zone KPIs
- aggregate hourly pickup hotspots and revenue concentration
- flag suspicious or operationally interesting trips with rule-based anomaly detection
- emit a compact data quality report across bronze, silver, and gold outputs

## Architecture

This codebase follows a medallion architecture:

- `raw`: externally sourced files and lookup tables
- `bronze`: source-aligned records with validation metadata and ingestion lineage
- `silver`: cleaned and business-friendly trip records
- `gold`: analytics-ready marts and monitoring outputs

Processing is partitioned by `year` and `month` and stored as parquet in overwrite mode. Jobs are implemented as independent Spark entrypoints and share common runtime wiring through:

- `JobRunner`: loads configuration, creates the Spark session, runs the job, and stops Spark
- `SparkSessionFactory`: applies Spark runtime settings from `application.conf`
- `DataFrameOps` and transformation modules: keep transformation logic separate from job orchestration
- `PartitionedParquetWriter`: standardizes partitioned parquet output

## Technology stack

- Scala 2.13.16
- Apache Spark 4.0.1 (`spark-core`, `spark-sql`)
- SBT
- Typesafe Config
- ScalaTest

## Repository layout

```text
src/main/scala/com/fleetops/analytics/
  common/          Shared Spark runtime, processing window, writers, helpers
  config/          Typed configuration loaders
  domain/          Schemas, models, job arguments
  job/
    bronze/        Raw ingestion jobs
    silver/        Cleansing and enrichment jobs
    gold/          Aggregations, anomaly detection, quality reporting
  transformation/  Pure transformation logic used by jobs

src/main/resources/
  application.conf Default local configuration

data/
  raw/             Source parquet and lookup CSV files
  bronze/          Bronze layer outputs
  silver/          Silver layer outputs
  gold/            Gold layer outputs
```

## Input data

Current local inputs in the repository:

- raw trip file: `data/raw/2025/yellow_tripdata_2025-01.parquet`
- taxi zone lookup: `data/raw/lookup/taxi_zone_lookup.csv`
- reference schema note: `data/raw/schema/yellow_tripdata_2025-01.md`

The source schema expected by bronze ingestion is defined in `YellowTripSourceSchema` and matches the yellow taxi parquet columns such as:

- vendor and trip timestamps
- pickup and dropoff location IDs
- passenger, distance, payment, and fare fields
- surcharge and total amount fields

Bronze ingestion performs a strict schema match before processing.

## Pipeline layers and datasets

### Bronze

Output path: `data/bronze/trips`

Purpose:

- ingest raw parquet
- validate source rows
- add ingestion metadata
- partition records by `year` and `month`

Validation rules currently implemented:

- pickup time must be before dropoff time
- trip distance must not be negative or `NaN`
- fare amount must not be negative or `NaN`

Bronze output columns:

- all source yellow taxi columns
- `validation_errors: array<string>`
- `ingestion_timestamp: timestamp_ntz`
- `source_file: string`
- `year: int`
- `month: int`

### Silver

#### Normalized trips

Output path: `data/silver/trips`

Purpose:

- drop invalid bronze records
- rename and cast source columns into analytics-friendly names
- derive trip duration in seconds

Output columns:

- `vendorId`
- `pickupDatetime`
- `dropoffDatetime`
- `pickupLocationId`
- `dropoffLocationId`
- `passengerCount`
- `tripDistance`
- `fareAmount`
- `tipAmount`
- `totalAmount`
- `paymentType`
- `tripDurationSeconds`
- `year`
- `month`

#### Enriched trips

Output path: `data/silver/trips_enriched`

Purpose:

- join the silver trips dataset with taxi zone lookup metadata
- add human-readable pickup and dropoff zone attributes

Additional output columns on top of normalized trips:

- `pickupBorough`
- `pickupZone`
- `dropoffBorough`
- `dropoffZone`

The lookup join is broadcast in Spark, which is appropriate for the small taxi zone reference dataset.

### Gold

#### Daily zone metrics

Output path: `data/gold/daily_zone_metrics`

Purpose:

- provide per-day, per-pickup-zone operational KPIs

Output schema:

- `tripDate`
- `pickupLocationId`
- `pickupBorough`
- `pickupZone`
- `tripsCount`
- `totalTripDistance`
- `avgTripDistance`
- `totalFareAmount`
- `avgFareAmount`
- `totalTipAmount`
- `totalRevenue`
- `avgTripDurationSeconds`
- `year`
- `month`

#### Hourly hotspot metrics

Output path: `data/gold/hourly_hotspots`

Purpose:

- identify high-traffic pickup zones by hour
- measure revenue concentration across zones and hours

Output schema:

- `tripDate`
- `pickupHour`
- `pickupLocationId`
- `pickupBorough`
- `pickupZone`
- `tripsCount`
- `totalRevenue`
- `avgRevenuePerTrip`
- `year`
- `month`

This dataset is sorted within partitions by date, hour, descending trip volume, and pickup location ID.

#### Trip anomalies

Output path: `data/gold/trip_anomalies`

Purpose:

- flag trips that match configurable anomaly rules

Selected output columns:

- `pickupDatetime`
- `pickupLocationId`
- `pickupZone`
- `dropoffLocationId`
- `dropoffZone`
- `tripDistance`
- `fareAmount`
- `totalAmount`
- `tipAmount`
- `tripDurationSeconds`
- `anomalyFlags`
- `isAnomalous`
- `year`
- `month`

Current anomaly rules are driven by configuration:

- very long duration
- very high fare
- very high tip
- low distance with high fare

#### Data quality report

Output path: `data/gold/data_quality_report`

Purpose:

- summarize data quality and record flow across the pipeline for one processing window

Output schema:

- `processingDate`
- `year`
- `month`
- `datasetName`
- `metricName`
- `metricValue`

Current metrics include:

- bronze total records, invalid records, invalid ratio
- silver normalized records, dropped invalid records, normalized ratio
- silver enrichment missing pickup/dropoff zone counts and ratios
- gold anomaly counts and ratios

## Runnable jobs

All jobs are standalone Spark entrypoints under `src/main/scala/com/fleetops/analytics/job`.

### Local pipeline orchestration

Use the local Python orchestrator to run the full pipeline in the required dependency order:

```bash
./scripts/run_pipeline_local.py --year 2025 --month 01
```

or in debug mode 

```bash
./scripts/run_pipeline_local.py --year 2025 --month 01 --debug
```

Behavior:

- runs bronze, silver, and gold jobs sequentially
- stops immediately when any job exits with a non-zero status
- logs each job start, command, and completion timestamp clearly
- overrides `fleetops.dataset.year` and `fleetops.dataset.month` at runtime for all non-bronze jobs

By default the bronze input path is resolved as:

```text
data/raw/<year>/yellow_tripdata_<year>-<month>.parquet
```

Example:

```text
data/raw/2025/yellow_tripdata_2025-01.parquet
```

If the raw parquet file lives elsewhere, pass it explicitly:

```bash
./scripts/run_pipeline_local.py \
  --year 2025 \
  --month 01 \
  --input-path /absolute/path/to/yellow_tripdata_2025-01.parquet
```

### Recommended local execution order

```bash
sbt "runMain com.fleetops.analytics.job.bronze.BronzeTripIngestionJob --input-path data/raw/2025/yellow_tripdata_2025-01.parquet --year 2025 --month 1"
sbt "runMain com.fleetops.analytics.job.silver.SilverTripNormalizationJob"
sbt "runMain com.fleetops.analytics.job.silver.SilverTripEnrichmentJob"
sbt "runMain com.fleetops.analytics.job.gold.DailyZoneMetricsJob"
sbt "runMain com.fleetops.analytics.job.gold.HourlyHotspotMetricsJob"
sbt "runMain com.fleetops.analytics.job.gold.TripAnomalyDetectionJob"
sbt "runMain com.fleetops.analytics.job.gold.DataQualityReportJob"
```

## Build and test

Prerequisites:

- JDK 17+
- SBT 1.10+

Commands:

```bash
sbt compile
sbt test
```

## Airflow orchestration

An Airflow DAG is available at `scripts/fleetops_pipeline_dag.py`.

What it does:

- creates one Airflow task for each current Spark job
- preserves the existing dependency order from the local orchestrator
- allows the three gold aggregations to run in parallel after silver enrichment
- finishes with the data quality report after anomaly detection completes

Default DAG params:

- `year` and `month` come from the DAG run's logical month
- `input_path=null`, which falls back to `data/raw/<year>/yellow_tripdata_<year>-<month>.parquet`
- `sbt_bin=sbt`

Example manual trigger payload:

```json
{
  "year": 2025,
  "month": 1,
  "input_path": "~/fleetops-analytics/data/raw/2025/yellow_tripdata_2025-01.parquet"
}
```
