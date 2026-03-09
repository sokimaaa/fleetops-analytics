package com.fleetops.analytics.config

final case class SparkConfig(
  appName: String,
  master: String,
  shufflePartitions: Int,
  logLevel: String,
  partitionOverwriteMode: String
)
