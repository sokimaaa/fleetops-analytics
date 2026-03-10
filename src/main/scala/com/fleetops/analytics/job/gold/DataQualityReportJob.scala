package com.fleetops.analytics.job.gold

import com.fleetops.analytics.common.ops.DataFrameOps._
import com.fleetops.analytics.common.ops.WriterOps.ParquetWriterOps
import com.fleetops.analytics.common.{JobRunner, ProcessingWindow, SparkJob}
import com.fleetops.analytics.config.AppConfig
import com.fleetops.analytics.transformation.DataQualityReportTransformation
import org.apache.spark.sql.SparkSession

import java.time.LocalDate

object DataQualityReportJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit spark: SparkSession, appConfig: AppConfig): Unit = {
    timed("DataQualityReportJob aggregation") {
      val processingWindow = ProcessingWindow.from(appConfig.dataset)
      val outputPath = s"${appConfig.storage.gold}/data_quality_report"

      val bronzeDf = spark.read.parquet(s"${appConfig.storage.bronze}/trips")
        .forProcessingWindow(processingWindow)

      val normalizedDf = spark.read.parquet(s"${appConfig.storage.silver}/trips")
        .forProcessingWindow(processingWindow)

      val enrichedDf = spark.read.parquet(s"${appConfig.storage.silver}/trips_enriched")
        .forProcessingWindow(processingWindow)

      val anomalyDf = spark.read.parquet(s"${appConfig.storage.gold}/trip_anomalies")
        .forProcessingWindow(processingWindow)

      DataQualityReportTransformation.build(
        bronzeDf = bronzeDf,
        normalizedDf = normalizedDf,
        enrichedDf = enrichedDf,
        anomalyDf = anomalyDf,
        processingDate = LocalDate.now().toString,
        year = processingWindow.year,
        month = processingWindow.month
      )
        .coalesce(1)
        .writeParquet(outputPath, Seq("year", "month"))
    }
  }
}
