package com.fleetops.analytics.job.gold

import com.fleetops.analytics.common.{JobRunner, SparkJob}
import com.fleetops.analytics.config.AppConfig
import com.fleetops.analytics.transformation.ops.DataFrameOps._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

object DailyZoneMetricsJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit spark: SparkSession, appConfig: AppConfig): Unit = {
    timed("DailyZoneMetricsJob aggregation") {
      val inputPath = s"${appConfig.storage.silver}/trips_enriched"
      val outputPath = s"${appConfig.storage.gold}/daily_zone_metrics"

      spark.read.parquet(inputPath)
        .select(
          col("pickupDatetime"),
          col("pickupLocationId"),
          col("pickupBorough"),
          col("pickupZone"),
          col("tripDistance"),
          col("fareAmount"),
          col("tipAmount"),
          col("totalAmount"),
          col("tripDurationSeconds")
        )
        .toDailyZoneMetrics
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("year", "month")
        .parquet(outputPath)
    }
  }
}
