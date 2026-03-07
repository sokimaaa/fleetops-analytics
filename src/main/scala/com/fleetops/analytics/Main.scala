package com.fleetops.analytics

import com.fleetops.analytics.common.{JobRunner, SparkJob}
import com.fleetops.analytics.config.AppConfig
import org.apache.spark.sql.SparkSession

object Main extends SparkJob {
  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(spark: SparkSession, appConfig: AppConfig, args: Array[String]): Unit = {
    logInfo("FleetOps Analytics pipeline bootstrap is ready.")
    logInfo(
      s"Storage layout: raw=${appConfig.storage.raw}, bronze=${appConfig.storage.bronze}, silver=${appConfig.storage.silver}, gold=${appConfig.storage.gold}"
    )
  }
}
