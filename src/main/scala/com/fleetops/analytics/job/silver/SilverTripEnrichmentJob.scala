package com.fleetops.analytics.job.silver

import com.fleetops.analytics.common.ops.DataFrameOps._
import com.fleetops.analytics.common.ops.WriterOps.ParquetWriterOps
import com.fleetops.analytics.common.{JobRunner, ProcessingWindow, SparkJob}
import com.fleetops.analytics.config.AppConfig
import org.apache.spark.sql.SparkSession

object SilverTripEnrichmentJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit spark: SparkSession, appConfig: AppConfig): Unit = {
    timed("SilverTripEnrichmentJob enrichment") {
      val tripsInputPath = s"${appConfig.storage.silver}/trips"
      val zoneLookupInputPath = s"${appConfig.storage.raw}/lookup/taxi_zone_lookup.csv"
      val outputPath = s"${appConfig.storage.silver}/trips_enriched"
      val processingWindow = ProcessingWindow.from(appConfig.dataset)

      val zoneLookupDf = spark.read
        .option("header", "true")
        .csv(zoneLookupInputPath)

      spark.read.parquet(tripsInputPath)
        .forProcessingWindow(processingWindow)
        .withTaxiZones(zoneLookupDf)
        .writeParquet(outputPath, Seq("year", "month"))
    }
  }
}
