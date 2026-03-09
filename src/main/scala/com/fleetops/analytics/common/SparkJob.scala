package com.fleetops.analytics.common

import com.fleetops.analytics.config.AppConfig
import org.apache.spark.sql.SparkSession

trait SparkJob extends TimedLogging {

  def run(args: Array[String])(implicit ss: SparkSession, config: AppConfig): Unit
}
