package com.fleetops.analytics.config

final case class AnomalyConfig(
  veryLongDurationSeconds: Int,
  veryHighFareAmount: Double,
  veryHighTipAmount: Double,
  lowDistanceKmThreshold: Double,
  lowDistanceHighFareAmount: Double
)
