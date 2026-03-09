package com.fleetops.analytics.job.silver

import com.fleetops.analytics.common.ops.DataFrameOps._
import com.fleetops.analytics.common.ops.WriterOps.ParquetWriterOps
import com.fleetops.analytics.common.{JobRunner, ProcessingWindow, SparkJob}
import com.fleetops.analytics.config.AppConfig
import org.apache.spark.sql.SparkSession

import scala.language.implicitConversions

object SilverTripNormalizationJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit spark: SparkSession, appConfig: AppConfig): Unit = {
    timed("SilverTripNormalizationJob normalization") {
      val inputPath = s"${appConfig.storage.bronze}/trips"
      val outputPath = s"${appConfig.storage.silver}/trips"
      val processingWindow = ProcessingWindow.from(appConfig.dataset)

      spark.read.parquet(inputPath)
        .forProcessingWindow(processingWindow)
        .normalize
        .writeParquet(outputPath, Seq("year", "month"))
    }
  }
}
