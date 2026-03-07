package com.fleetops.analytics.config

final case class StorageConfig(
  raw: String,
  bronze: String,
  silver: String,
  gold: String
)
