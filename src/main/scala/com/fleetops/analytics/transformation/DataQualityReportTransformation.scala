package com.fleetops.analytics.transformation

import com.fleetops.analytics.domain.schema.DataQualityReportSchema
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object DataQualityReportTransformation {

  def build(
    bronzeDf: DataFrame,
    normalizedDf: DataFrame,
    enrichedDf: DataFrame,
    anomalyDf: DataFrame,
    processingDate: String,
    year: Int,
    month: Int
  )(implicit spark: SparkSession): DataFrame = {
    val bronzeStats = bronzeDf.agg(
      count(lit(1)).as("totalRecords"),
      coalesce(sum(when(size(col("validation_errors")) > 0, 1).otherwise(0)).cast("long"), lit(0L)).as("invalidRecords")
    )

    val normalizedStats = normalizedDf.agg(
      count(lit(1)).as("normalizedRecords")
    )

    val enrichedStats = enrichedDf.agg(
      count(lit(1)).as("enrichedRecords"),
      coalesce(sum(when(col("pickupZone").isNull, 1).otherwise(0)).cast("long"), lit(0L)).as("missingPickupZoneCount"),
      coalesce(sum(when(col("dropoffZone").isNull, 1).otherwise(0)).cast("long"), lit(0L)).as("missingDropoffZoneCount")
    )

    val anomalyStats = anomalyDf.agg(
      count(lit(1)).as("anomalousTripsCount")
    )

    val totals = bronzeStats
      .crossJoin(normalizedStats)
      .crossJoin(enrichedStats)
      .crossJoin(anomalyStats)
      .select(
        col("totalRecords"),
        col("invalidRecords"),
        col("normalizedRecords"),
        col("enrichedRecords"),
        col("missingPickupZoneCount"),
        col("missingDropoffZoneCount"),
        col("anomalousTripsCount")
      )
      .head()

    val totalRecords = totals.getAs[Long]("totalRecords")
    val invalidRecords = totals.getAs[Long]("invalidRecords")
    val normalizedRecords = totals.getAs[Long]("normalizedRecords")
    val enrichedRecords = totals.getAs[Long]("enrichedRecords")
    val missingPickupZoneCount = totals.getAs[Long]("missingPickupZoneCount")
    val missingDropoffZoneCount = totals.getAs[Long]("missingDropoffZoneCount")
    val anomalousTripsCount = totals.getAs[Long]("anomalousTripsCount")
    val droppedInvalidRecords = totalRecords - normalizedRecords

    val rows = Seq(
      metricRow(processingDate, year, month, "bronze_validation", "totalRecords", totalRecords.toDouble),
      metricRow(processingDate, year, month, "bronze_validation", "invalidRecords", invalidRecords.toDouble),
      metricRow(processingDate, year, month, "bronze_validation", "invalidRecordRatio", ratio(invalidRecords, totalRecords)),
      metricRow(processingDate, year, month, "silver_normalized_trips", "normalizedRecords", normalizedRecords.toDouble),
      metricRow(processingDate, year, month, "silver_normalized_trips", "droppedInvalidRecords", droppedInvalidRecords.toDouble),
      metricRow(processingDate, year, month, "silver_normalized_trips", "normalizedRecordRatio", ratio(normalizedRecords, totalRecords)),
      metricRow(processingDate, year, month, "silver_enriched_trips", "missingPickupZoneCount", missingPickupZoneCount.toDouble),
      metricRow(processingDate, year, month, "silver_enriched_trips", "missingDropoffZoneCount", missingDropoffZoneCount.toDouble),
      metricRow(processingDate, year, month, "silver_enriched_trips", "missingPickupZoneRatio", ratio(missingPickupZoneCount, enrichedRecords)),
      metricRow(processingDate, year, month, "silver_enriched_trips", "missingDropoffZoneRatio", ratio(missingDropoffZoneCount, enrichedRecords)),
      metricRow(processingDate, year, month, "gold_trip_anomalies", "anomalousTripsCount", anomalousTripsCount.toDouble),
      metricRow(processingDate, year, month, "gold_trip_anomalies", "anomalousTripsRatio", ratio(anomalousTripsCount, enrichedRecords))
    )

    spark.createDataFrame(spark.sparkContext.parallelize(rows), DataQualityReportSchema.structType)
  }

  private def ratio(numerator: Long, denominator: Long): Double =
    if (denominator == 0L) 0.0 else numerator.toDouble / denominator.toDouble

  private def metricRow(
    processingDate: String,
    year: Int,
    month: Int,
    datasetName: String,
    metricName: String,
    metricValue: Double
  ): Row = Row(
    Date.valueOf(processingDate),
    year,
    month,
    datasetName,
    metricName,
    metricValue
  )
}
