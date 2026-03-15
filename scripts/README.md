# Airflow DAGs

`fleetops_pipeline_dag.py` orchestrates every current FleetOps Spark job in the same order as `scripts/run_pipeline_local.py`.

Default behavior:

- `year` and `month` come from the DAG run's logical month
- `input_path=None` resolves to `data/raw/<year>/yellow_tripdata_<year>-<month>.parquet`
- `sbt_bin=sbt`

Override `year`, `month`, `input_path`, or `sbt_bin` in `dag_run.conf` when you need a different processing window or raw input path.
