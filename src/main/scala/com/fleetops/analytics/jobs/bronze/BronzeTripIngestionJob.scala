package com.fleetops.analytics.jobs.bronze

import com.fleetops.analytics.common.{JobRunner, SparkJob}
import com.fleetops.analytics.config.AppConfig
import com.fleetops.analytics.domain.args.BronzeJobArgs
import com.fleetops.analytics.domain.schema.YellowTripSourceSchema
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, format_string, input_file_name, lit, lpad, month, year}
import org.apache.spark.sql.{SaveMode, SparkSession}

object BronzeTripIngestionJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(spark: SparkSession, appConfig: AppConfig, args: Array[String]): Unit = {
    timed("BronzeTripIngestionJob ingestion") {
      val jobArgs = BronzeJobArgs(args)
      val outputPath = s"${appConfig.storage.bronze}/trips"

      val rawDf = spark.read.parquet(jobArgs.inputPath)
      YellowTripSourceSchema.requireMatch(rawDf.schema)

      val recordsRead = rawDf.count()
      logInfo(s"Records read: $recordsRead from ${jobArgs.inputPath}")

      rawDf
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn(
          "year",
          coalesce(year(col("tpep_pickup_datetime")), lit(jobArgs.year))
        )
        .withColumn(
          "month",
          coalesce(month(col("tpep_pickup_datetime")), lit(jobArgs.month))
        )
        .write.mode(SaveMode.Overwrite)
        .partitionBy("year", "month")
        .parquet(outputPath)
    }
  }
}
