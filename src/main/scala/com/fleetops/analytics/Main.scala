package com.fleetops.analytics

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("FleetOps Analytics")
      .master("local[*]")
      .getOrCreate()

    println("FleetOps Analytics pipeline bootstrap is ready.")

    spark.stop()
  }
}
