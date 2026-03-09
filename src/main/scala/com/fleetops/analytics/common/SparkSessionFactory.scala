package com.fleetops.analytics.common

import com.fleetops.analytics.config.AppConfig
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def create(appConfig: AppConfig): SparkSession = {
    val sparkConfig = appConfig.spark
    val spark = SparkSession
      .builder()
      .appName(sparkConfig.appName)
      .master(sparkConfig.master)
      .config("spark.sql.shuffle.partitions", sparkConfig.shufflePartitions.toString)
      .config("spark.sql.sources.partitionOverwriteMode", sparkConfig.partitionOverwriteMode)
      .getOrCreate()

    spark.sparkContext.setLogLevel(sparkConfig.logLevel)
    spark
  }
}
