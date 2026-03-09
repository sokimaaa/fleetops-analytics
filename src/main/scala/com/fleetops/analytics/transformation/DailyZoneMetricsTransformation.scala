package com.fleetops.analytics.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DailyZoneMetricsTransformation {

  def aggregate(enrichedTripsDf: DataFrame): DataFrame =
    enrichedTripsDf
      .withColumn("tripDate", to_date(col("pickupDatetime")))
      .filter(col("tripDate").isNotNull)
      .groupBy(
        col("tripDate"),
        col("pickupLocationId"),
        col("pickupBorough"),
        col("pickupZone")
      )
      .agg(
        count(lit(1)).as("tripsCount"),
        sum(col("tripDistance")).as("totalTripDistance"),
        avg(col("tripDistance")).as("avgTripDistance"),
        sum(col("fareAmount")).as("totalFareAmount"),
        avg(col("fareAmount")).as("avgFareAmount"),
        sum(col("tipAmount")).as("totalTipAmount"),
        sum(col("totalAmount")).as("totalRevenue"),
        avg(col("tripDurationSeconds")).as("avgTripDurationSeconds")
      )
      .withColumn("year", year(col("tripDate")))
      .withColumn("month", month(col("tripDate")))
      .select(
        col("tripDate"),
        col("pickupLocationId"),
        col("pickupBorough"),
        col("pickupZone"),
        col("tripsCount"),
        col("totalTripDistance"),
        col("avgTripDistance"),
        col("totalFareAmount"),
        col("avgFareAmount"),
        col("totalTipAmount"),
        col("totalRevenue"),
        col("avgTripDurationSeconds"),
        col("year"),
        col("month")
      )
}
