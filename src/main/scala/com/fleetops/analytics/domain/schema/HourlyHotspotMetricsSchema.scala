package com.fleetops.analytics.domain.schema

import org.apache.spark.sql.types._

object HourlyHotspotMetricsSchema {
  val structType: StructType = StructType(
    Seq(
      StructField("tripDate", DateType, nullable = true),
      StructField("pickupHour", IntegerType, nullable = true),
      StructField("pickupLocationId", IntegerType, nullable = false),
      StructField("pickupBorough", StringType, nullable = true),
      StructField("pickupZone", StringType, nullable = true),
      StructField("tripsCount", LongType, nullable = false),
      StructField("totalRevenue", DoubleType, nullable = true),
      StructField("avgRevenuePerTrip", DoubleType, nullable = true),
      StructField("year", IntegerType, nullable = true),
      StructField("month", IntegerType, nullable = true)
    )
  )
}
