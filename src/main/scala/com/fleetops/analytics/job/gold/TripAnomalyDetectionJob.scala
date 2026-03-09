package com.fleetops.analytics.job.gold

import com.fleetops.analytics.common.ops.DataFrameOps._
import com.fleetops.analytics.common.ops.WriterOps.ParquetWriterOps
import com.fleetops.analytics.common.{JobRunner, ProcessingWindow, SparkJob}
import com.fleetops.analytics.config.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object TripAnomalyDetectionJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit ss: SparkSession, config: AppConfig): Unit = {
    timed("AnomalyTripDetectionJob aggregation") {
      val inputPath = s"${config.storage.silver}/trips_enriched"
      val outputPath = s"${config.storage.gold}/trip_anomalies"
      val processingWindow = ProcessingWindow.from(config.dataset)

      ss.read.parquet(inputPath)
        .forProcessingWindow(processingWindow)
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
        .writeParquet(outputPath, Seq("year", "month"))
    }
  }
}
