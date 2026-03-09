package com.fleetops.analytics.job.gold

import com.fleetops.analytics.common.{JobRunner, SparkJob}
import com.fleetops.analytics.config.AppConfig
import com.fleetops.analytics.transformation.ops.DataFrameOps._
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.{SaveMode, SparkSession}

object HourlyHotspotMetricsJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit spark: SparkSession, appConfig: AppConfig): Unit = {
    timed("HourlyHotspotMetricsJob aggregation") {
      val inputPath = s"${appConfig.storage.silver}/trips_enriched"
      val outputPath = s"${appConfig.storage.gold}/hourly_hotspots"

      spark.read.parquet(inputPath)
        .select(
          col("pickupDatetime"),
          col("pickupLocationId"),
          col("pickupBorough"),
          col("pickupZone"),
          col("totalAmount")
        )
        .toHourlyHotspotMetrics
        .repartition(col("year"), col("month"))
        .sortWithinPartitions(
          col("tripDate").asc,
          col("pickupHour").asc,
          desc("tripsCount"),
          col("pickupLocationId").asc
        )
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("year", "month")
        .parquet(outputPath)
    }
  }
}
