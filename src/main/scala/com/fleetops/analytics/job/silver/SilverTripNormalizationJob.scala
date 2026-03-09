package com.fleetops.analytics.job.silver

import com.fleetops.analytics.common.{JobRunner, SparkJob}
import com.fleetops.analytics.config.AppConfig
import com.fleetops.analytics.transformation.ops.DataFrameOps._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.language.implicitConversions

object SilverTripNormalizationJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit spark: SparkSession, appConfig: AppConfig): Unit = {
    timed("SilverTripNormalizationJob normalization") {
      val inputPath = s"${appConfig.storage.bronze}/trips"
      val outputPath = s"${appConfig.storage.silver}/trips"

      spark.read.parquet(inputPath)
        .normalize
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("year", "month")
        .parquet(outputPath)
    }
  }
}
