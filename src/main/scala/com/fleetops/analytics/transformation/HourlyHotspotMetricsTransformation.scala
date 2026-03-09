package com.fleetops.analytics.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object HourlyHotspotMetricsTransformation {

  def aggregate(enrichedTripsDf: DataFrame): DataFrame =
    enrichedTripsDf
      .withColumn("tripDate", to_date(col("pickupDatetime")))
      .withColumn("pickupHour", hour(col("pickupDatetime")))
      .filter(col("tripDate").isNotNull && col("pickupHour").isNotNull)
      .groupBy(
        col("tripDate"),
        col("pickupHour"),
        col("pickupLocationId"),
        col("pickupBorough"),
        col("pickupZone")
      )
      .agg(
        count(lit(1)).as("tripsCount"),
        sum(col("totalAmount")).as("totalRevenue"),
        avg(col("totalAmount")).as("avgRevenuePerTrip")
      )
      .withColumn("year", year(col("tripDate")))
      .withColumn("month", month(col("tripDate")))
      .select(
        col("tripDate"),
        col("pickupHour"),
        col("pickupLocationId"),
        col("pickupBorough"),
        col("pickupZone"),
        col("tripsCount"),
        col("totalRevenue"),
        col("avgRevenuePerTrip"),
        col("year"),
        col("month")
      )
}
