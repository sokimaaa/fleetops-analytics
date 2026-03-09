package com.fleetops.analytics.domain.schema

import org.apache.spark.sql.types._

object DailyZoneMetricsSchema {
  val structType: StructType = StructType(
    Seq(
      StructField("tripDate", DateType, nullable = true),
      StructField("pickupLocationId", IntegerType, nullable = false),
      StructField("pickupBorough", StringType, nullable = true),
      StructField("pickupZone", StringType, nullable = true),
      StructField("tripsCount", LongType, nullable = false),
      StructField("totalTripDistance", DoubleType, nullable = true),
      StructField("avgTripDistance", DoubleType, nullable = true),
      StructField("totalFareAmount", DoubleType, nullable = true),
      StructField("avgFareAmount", DoubleType, nullable = true),
      StructField("totalTipAmount", DoubleType, nullable = true),
      StructField("totalRevenue", DoubleType, nullable = true),
      StructField("avgTripDurationSeconds", DoubleType, nullable = true),
      StructField("year", IntegerType, nullable = true),
      StructField("month", IntegerType, nullable = true)
    )
  )
}
