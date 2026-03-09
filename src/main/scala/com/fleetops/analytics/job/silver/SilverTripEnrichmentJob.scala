package com.fleetops.analytics.job.silver

import com.fleetops.analytics.common.{JobRunner, SparkJob}
import com.fleetops.analytics.config.AppConfig
import com.fleetops.analytics.transformation.ops.DataFrameOps._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SilverTripEnrichmentJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit spark: SparkSession, appConfig: AppConfig): Unit = {
    timed("SilverTripEnrichmentJob enrichment") {
      val tripsInputPath = s"${appConfig.storage.silver}/trips"
      val zoneLookupInputPath = s"${appConfig.storage.raw}/lookup/taxi_zone_lookup.csv"
      val outputPath = s"${appConfig.storage.silver}/trips_enriched"

      val zoneLookupDf = spark.read
        .option("header", "true")
        .csv(zoneLookupInputPath)

      spark.read.parquet(tripsInputPath)
        .withTaxiZones(zoneLookupDf)
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("year", "month")
        .parquet(outputPath)
    }
  }
}
