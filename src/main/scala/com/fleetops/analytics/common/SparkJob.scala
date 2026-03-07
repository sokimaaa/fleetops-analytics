package com.fleetops.analytics.common

import com.fleetops.analytics.config.AppConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

trait SparkJob extends Logging {

  def run(spark: SparkSession, config: AppConfig, args: Array[String]): Unit
}
