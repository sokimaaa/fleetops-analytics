package com.fleetops.analytics.job.gold

import com.fleetops.analytics.common.ops.DataFrameOps._
import com.fleetops.analytics.common.ops.WriterOps.ParquetWriterOps
import com.fleetops.analytics.common.{JobRunner, ProcessingWindow, SparkJob}
import com.fleetops.analytics.config.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DailyZoneMetricsJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit spark: SparkSession, appConfig: AppConfig): Unit = {
    timed("DailyZoneMetricsJob aggregation") {
      val inputPath = s"${appConfig.storage.silver}/trips_enriched"
      val outputPath = s"${appConfig.storage.gold}/daily_zone_metrics"
      val processingWindow = ProcessingWindow.from(appConfig.dataset)

      spark.read.parquet(inputPath)
        .forProcessingWindow(processingWindow)
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
        .writeParquet(outputPath, Seq("year", "month"))
    }
  }
}
