package com.fleetops.analytics.config

import com.typesafe.config.{Config, ConfigFactory}

trait ConfigLoader[C] {
  def load(config: Config): C
}

object ConfigLoader {
  def load(): AppConfig = {
    val config = ConfigFactory.load()
    FleetOpsConfigLoader.load(config)
  }
}

object FleetOpsConfigLoader extends ConfigLoader[AppConfig] {
  private val RootPath = "fleetops"

  def load(config: Config): AppConfig = {
    val fleetopsConfig = config.getConfig(RootPath)
    AppConfig(
      storage = StorageConfigLoader.load(fleetopsConfig),
      dataset = DatasetConfigLoader.load(fleetopsConfig),
      spark = SparkConfigLoader.load(fleetopsConfig)
    )
  }
}

object StorageConfigLoader extends ConfigLoader[StorageConfig] {
  private val StoragePath = "storage"

  override def load(config: Config): StorageConfig = loadStorageConfig(config.getConfig(StoragePath))

  private def loadStorageConfig(config: Config): StorageConfig =
    StorageConfig(
      raw = config.getString("raw"),
      bronze = config.getString("bronze"),
      silver = config.getString("silver"),
      gold = config.getString("gold")
    )
}

object DatasetConfigLoader extends ConfigLoader[DatasetConfig] {
  private val DatasetPath = "dataset"

  override def load(config: Config): DatasetConfig = loadDatasetConfig(config.getConfig(DatasetPath))

  private def loadDatasetConfig(config: Config): DatasetConfig =
    DatasetConfig(
      taxiType = config.getString("taxiType"),
      year = config.getInt("year"),
      month = config.getInt("month")
    )
}

object SparkConfigLoader extends ConfigLoader[SparkConfig] {
  private val SparkPath = "spark"

  override def load(config: Config): SparkConfig = loadSparkConfig(config.getConfig(SparkPath))

  private def loadSparkConfig(config: Config): SparkConfig =
    SparkConfig(
      appName = config.getString("appName"),
      master = config.getString("master"),
      shufflePartitions = config.getInt("shufflePartitions"),
      logLevel = if (config.hasPath("logLevel")) config.getString("logLevel") else "WARN"
    )
}
