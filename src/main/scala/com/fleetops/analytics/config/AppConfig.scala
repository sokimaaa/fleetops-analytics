package com.fleetops.analytics.config

final case class AppConfig(
  storage: StorageConfig,
  dataset: DatasetConfig,
  spark: SparkConfig
)
