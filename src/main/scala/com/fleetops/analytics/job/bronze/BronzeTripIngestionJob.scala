package com.fleetops.analytics.job.bronze

import com.fleetops.analytics.common.io.PartitionedParquetWriter
import com.fleetops.analytics.common.ops.DataFrameOps._
import com.fleetops.analytics.common.{JobRunner, ProcessingWindow, SparkJob}
import com.fleetops.analytics.config.AppConfig
import com.fleetops.analytics.domain.args.BronzeJobArgs
import com.fleetops.analytics.domain.schema.YellowTripSourceSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BronzeTripIngestionJob extends SparkJob {

  def main(args: Array[String]): Unit = JobRunner.run(this, args)

  override def run(args: Array[String])(implicit spark: SparkSession, appConfig: AppConfig): Unit = {
    timed("BronzeTripIngestionJob ingestion") {
      val jobArgs = BronzeJobArgs(args)
      val outputPath = s"${appConfig.storage.bronze}/trips"
      val processingWindow = ProcessingWindow(jobArgs.year, jobArgs.month)

      val rawDf = spark.read.parquet(jobArgs.inputPath)
      YellowTripSourceSchema.requireMatch(rawDf.schema)

      val preparedDf = rawDf
        .withValidationErrors
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
        .forProcessingWindow(processingWindow)

      val metrics = preparedDf.agg(
        count(lit(1)).as("total_records"),
        sum(when(size(col("validation_errors")) > 0, 1).otherwise(0)).as("invalid_records")
      ).first

      val totalRecords = metrics.getAs[Long]("total_records")
      val invalidRecords = metrics.getAs[Long]("invalid_records")

      val invalidRecordPercentage =
        if (totalRecords == 0) 0.0
        else invalidRecords.toDouble * 100.0 / totalRecords.toDouble


      logInfo(s"Records read: ${metrics.getAs("total_records")} from ${jobArgs.inputPath}")
      logInfo(
        f"Bronze validation summary: total_records=$totalRecords%,d, invalid_records_count=$invalidRecords%,d, invalid_record_percentage=$invalidRecordPercentage%.3f%%"
      )

      PartitionedParquetWriter.write(preparedDf)(outputPath, Seq("year", "month"))
    }
  }
}
