#!/usr/bin/env python3
"""Run the local FleetOps batch pipeline in dependency order."""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class PipelineJob:
    name: str
    main_class: str
    args: tuple[str, ...] = ()


PIPELINE_JOBS = (
    PipelineJob(
        name="bronze-ingestion",
        main_class="com.fleetops.analytics.job.bronze.BronzeTripIngestionJob",
    ),
    PipelineJob(
        name="silver-normalization",
        main_class="com.fleetops.analytics.job.silver.SilverTripNormalizationJob",
    ),
    PipelineJob(
        name="silver-enrichment",
        main_class="com.fleetops.analytics.job.silver.SilverTripEnrichmentJob",
    ),
    PipelineJob(
        name="gold-daily-zone-metrics",
        main_class="com.fleetops.analytics.job.gold.DailyZoneMetricsJob",
    ),
    PipelineJob(
        name="gold-hourly-hotspots",
        main_class="com.fleetops.analytics.job.gold.HourlyHotspotMetricsJob",
    ),
    PipelineJob(
        name="gold-trip-anomalies",
        main_class="com.fleetops.analytics.job.gold.TripAnomalyDetectionJob",
    ),
    PipelineJob(
        name="gold-data-quality-report",
        main_class="com.fleetops.analytics.job.gold.DataQualityReportJob",
    ),
)


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run all FleetOps Spark jobs locally in the required order."
    )
    parser.add_argument("--year", type=int, required=True, help="Processing year, for example 2025.")
    parser.add_argument("--month", type=int, required=True, help="Processing month, for example 1 or 01.")
    parser.add_argument(
        "--input-path",
        help=(
            "Optional raw parquet path for the bronze job. "
            "Defaults to data/raw/<year>/yellow_tripdata_<year>-<month>.parquet."
        ),
    )
    parser.add_argument(
        "--sbt-bin",
        default="sbt",
        help="SBT executable to use. Defaults to `sbt`.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Stream SBT and Spark logs to the console.",
    )
    return parser


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def validate_year_month(year: int, month: int) -> None:
    if year < 1:
        raise ValueError("--year must be a positive integer")
    if month < 1 or month > 12:
        raise ValueError("--month must be between 1 and 12")


def default_input_path(repo_root: Path, year: int, month: int) -> Path:
    return repo_root / "data" / "raw" / str(year) / f"yellow_tripdata_{year}-{month:02d}.parquet"


def format_command(command: list[str]) -> str:
    return shlex.join(command)


def build_sbt_environment(repo_root: Path) -> dict[str, str]:
    cache_root = repo_root / ".pipeline-runtime"
    sbt_boot = cache_root / "sbt" / "boot"
    sbt_global = cache_root / "sbt" / "global"
    ivy_home = cache_root / "ivy2"
    coursier_cache = cache_root / "coursier"

    for directory in (sbt_boot, sbt_global, ivy_home, coursier_cache):
        directory.mkdir(parents=True, exist_ok=True)

    environment = os.environ.copy()
    environment["COURSIER_CACHE"] = str(coursier_cache)

    sbt_opts = environment.get("SBT_OPTS", "").strip()
    managed_opts = [
        f"-Dsbt.boot.directory={sbt_boot}",
        f"-Dsbt.global.base={sbt_global}",
        f"-Dsbt.ivy.home={ivy_home}",
        f"-Dsbt.coursier.home={coursier_cache}",
    ]
    environment["SBT_OPTS"] = " ".join(filter(None, [sbt_opts, *managed_opts]))
    return environment


def cleanup_runtime_directory(repo_root: Path) -> None:
    runtime_dir = repo_root / ".pipeline-runtime"
    if not runtime_dir.exists():
        return

    shutil.rmtree(runtime_dir)
    log(f"Removed temporary runtime directory: {runtime_dir}")


def run_job(
    *,
    repo_root: Path,
    sbt_bin: str,
    year: int,
    month: int,
    input_path: Path,
    environment: dict[str, str],
    debug: bool,
    job: PipelineJob,
) -> None:
    command = [
        sbt_bin,
        "--batch",
        f"-Dfleetops.dataset.year={year}",
        f"-Dfleetops.dataset.month={month}",
    ]

    run_main_args = [job.main_class]
    if job.main_class.endswith("BronzeTripIngestionJob"):
        run_main_args.extend(
            ["--input-path", str(input_path), "--year", str(year), "--month", str(month)]
        )
    else:
        run_main_args.extend(job.args)

    command.append(f"runMain {' '.join(shlex.quote(arg) for arg in run_main_args)}")

    log(f"Starting job `{job.name}`")
    log(f"Command: {format_command(command)}")
    started_at = datetime.now()
    completed = subprocess.run(
        command,
        cwd=repo_root,
        env=environment,
        check=False,
        stdout=None if debug else subprocess.DEVNULL,
        stderr=None if debug else subprocess.DEVNULL,
    )
    duration = datetime.now() - started_at

    if completed.returncode != 0:
        raise subprocess.CalledProcessError(completed.returncode, command)

    log(f"Finished job `{job.name}` in {duration}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        validate_year_month(args.year, args.month)
    except ValueError as error:
        parser.error(str(error))

    repo_root = resolve_repo_root()
    input_path = (
        Path(args.input_path).expanduser().resolve()
        if args.input_path
        else default_input_path(repo_root, args.year, args.month).resolve()
    )

    if not input_path.exists():
        parser.error(f"Raw input file not found: {input_path}")

    log(
        "Running local pipeline for "
        f"year={args.year}, month={args.month:02d}, input_path={input_path}"
    )
    if args.debug:
        log("Debug logging enabled: streaming SBT and Spark output")

    try:
        environment = build_sbt_environment(repo_root)
        pipeline_started_at = datetime.now()

        try:
            for job in PIPELINE_JOBS:
                run_job(
                    repo_root=repo_root,
                    sbt_bin=args.sbt_bin,
                    year=args.year,
                    month=args.month,
                    input_path=input_path,
                    environment=environment,
                    debug=args.debug,
                    job=job,
                )
        except subprocess.CalledProcessError as error:
            log(f"Pipeline failed on command: {format_command(error.cmd)}")
            log(f"Exit code: {error.returncode}")
            return error.returncode

        log("Pipeline finished successfully")
        log(f"All jobs completed in {datetime.now() - pipeline_started_at}")
        return 0
    finally:
        cleanup_runtime_directory(repo_root)


if __name__ == "__main__":
    sys.exit(main())
