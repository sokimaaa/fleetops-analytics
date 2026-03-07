package com.fleetops.analytics

import com.fleetops.analytics.config.ConfigLoader
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val appConfig = ConfigLoader.load()

    val spark = SparkSession
      .builder()
      .appName(appConfig.spark.appName)
      .master(appConfig.spark.master)
      .config("spark.sql.shuffle.partitions", appConfig.spark.shufflePartitions.toString)
      .getOrCreate()

    println("FleetOps Analytics pipeline bootstrap is ready.")
    println(
      s"Storage layout: raw=${appConfig.storage.raw}, bronze=${appConfig.storage.bronze}, silver=${appConfig.storage.silver}, gold=${appConfig.storage.gold}"
    )

    spark.stop()
  }
}
