from __future__ import annotations

import shlex
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


REPO_ROOT = Path(__file__).resolve().parents[1]


def spark_job_task(
    *,
    dag: DAG,
    task_id: str,
    main_class: str,
    requires_bronze_args: bool = False,
) -> BashOperator:
    year = "{{ dag_run.conf.get('year', data_interval_start.year) if dag_run else data_interval_start.year }}"
    month = "{{ dag_run.conf.get('month', data_interval_start.month) if dag_run else data_interval_start.month }}"
    input_path = "{{ dag_run.conf.get('input_path', params.input_path) if dag_run else params.input_path }}"
    sbt_bin = "{{ dag_run.conf.get('sbt_bin', params.sbt_bin) if dag_run else params.sbt_bin }}"

    repo_root = str(REPO_ROOT)
    run_main_command = f"runMain {main_class}"
    if requires_bronze_args:
        run_main_command += ' --input-path \\"$INPUT_PATH\\" --year $YEAR --month $MONTH_RAW'

    bash_lines = [
        "set -euo pipefail",
        f"cd {shlex.quote(repo_root)}",
        f'SBT_BIN="{sbt_bin}"',
        f'YEAR="{year}"',
        f'MONTH_RAW="{month}"',
        'MONTH_PADDED=$(printf "%02d" "$MONTH_RAW")',
    ]
    if requires_bronze_args:
        bash_lines.extend(
            [
                f'INPUT_PATH="{input_path}"',
                'if [ -z "$INPUT_PATH" ] || [ "$INPUT_PATH" = "None" ]; then',
                f'  INPUT_PATH="{repo_root}/data/raw/$YEAR/yellow_tripdata_$YEAR-$MONTH_PADDED.parquet"',
                "fi",
            ]
        )
    bash_lines.append(
        '"$SBT_BIN" --batch '
        '"-Dfleetops.dataset.year=$YEAR" '
        '"-Dfleetops.dataset.month=$MONTH_RAW" '
        f'"{run_main_command}"'
    )

    return BashOperator(
        dag=dag,
        task_id=task_id,
        bash_command="\n".join(bash_lines),
    )


with DAG(
    dag_id="fleetops_analytics_pipeline",
    description="Run FleetOps bronze, silver, and gold Spark jobs in dependency order.",
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "fleetops"},
    params={
        "input_path": None,
        "sbt_bin": "sbt",
    },
    tags=["fleetops", "spark", "batch"],
) as dag:
    bronze_ingestion = spark_job_task(
        dag=dag,
        task_id="bronze_ingestion",
        main_class="com.fleetops.analytics.job.bronze.BronzeTripIngestionJob",
        requires_bronze_args=True,
    )
    silver_normalization = spark_job_task(
        dag=dag,
        task_id="silver_normalization",
        main_class="com.fleetops.analytics.job.silver.SilverTripNormalizationJob",
    )
    silver_enrichment = spark_job_task(
        dag=dag,
        task_id="silver_enrichment",
        main_class="com.fleetops.analytics.job.silver.SilverTripEnrichmentJob",
    )
    gold_daily_zone_metrics = spark_job_task(
        dag=dag,
        task_id="gold_daily_zone_metrics",
        main_class="com.fleetops.analytics.job.gold.DailyZoneMetricsJob",
    )
    gold_hourly_hotspots = spark_job_task(
        dag=dag,
        task_id="gold_hourly_hotspots",
        main_class="com.fleetops.analytics.job.gold.HourlyHotspotMetricsJob",
    )
    gold_trip_anomalies = spark_job_task(
        dag=dag,
        task_id="gold_trip_anomalies",
        main_class="com.fleetops.analytics.job.gold.TripAnomalyDetectionJob",
    )
    gold_data_quality_report = spark_job_task(
        dag=dag,
        task_id="gold_data_quality_report",
        main_class="com.fleetops.analytics.job.gold.DataQualityReportJob",
    )

    (
        bronze_ingestion
        >> silver_normalization
        >> silver_enrichment
        >> [
            gold_daily_zone_metrics,
            gold_hourly_hotspots,
            gold_trip_anomalies,
        ]
        >> gold_data_quality_report
    )
