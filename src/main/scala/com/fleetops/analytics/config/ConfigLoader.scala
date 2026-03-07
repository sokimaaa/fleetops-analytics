package com.fleetops.analytics.config

import com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {
  private val RootPath = "fleetops"

  def load(): AppConfig = {
    val config = ConfigFactory.load()
    load(config)
  }

  def load(config: Config): AppConfig = {
    val fleetopsConfig = config.getConfig(RootPath)
    AppConfig(
      storage = loadStorageConfig(fleetopsConfig.getConfig("storage")),
      dataset = loadDatasetConfig(fleetopsConfig.getConfig("dataset")),
      spark = loadSparkConfig(fleetopsConfig.getConfig("spark"))
    )
  }

  private def loadStorageConfig(config: Config): StorageConfig =
    StorageConfig(
      raw = config.getString("raw"),
      bronze = config.getString("bronze"),
      silver = config.getString("silver"),
      gold = config.getString("gold")
    )

  private def loadDatasetConfig(config: Config): DatasetConfig =
    DatasetConfig(
      taxiType = config.getString("taxiType"),
      year = config.getInt("year"),
      month = config.getInt("month")
    )

  private def loadSparkConfig(config: Config): SparkConfig =
    SparkConfig(
      appName = config.getString("appName"),
      master = config.getString("master"),
      shufflePartitions = config.getInt("shufflePartitions")
    )
}
