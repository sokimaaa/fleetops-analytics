package com.fleetops.analytics.job.gold

import com.fleetops.analytics.common.{JobRunner, SparkJob}
import com.fleetops.analytics.config.AppConfig
import com.fleetops.analytics.transformation.ops.DataFrameOps._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

object TripAnomalyDetectionJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit ss: SparkSession, config: AppConfig): Unit = {
    timed("AnomalyTripDetectionJob aggregation") {
      val inputPath = s"${config.storage.silver}/trips_enriched"
      val outputPath = s"${config.storage.gold}/trip_anomalies"

      ss.read.parquet(inputPath)
        .select(
          col("pickupDatetime"),
          col("pickupLocationId"),
          col("pickupZone"),
          col("dropoffLocationId"),
          col("dropoffZone"),
          col("tripDistance"),
          col("fareAmount"),
          col("totalAmount"),
          col("tipAmount"),
          col("tripDurationSeconds"),
          col("year"),
          col("month")
        )
        .withAnomalyRules
        .filterAnomalous
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("year", "month")
        .parquet(outputPath)
    }
  }
}
