package com.fleetops.analytics.job.gold

import com.fleetops.analytics.domain.schema.DataQualityReportSchema
import com.fleetops.analytics.transformation.DataQualityReportTransformation
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class DataQualityReportJobSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("DataQualityReportJobSpec")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  test("build computes bronze, silver, and gold quality metrics for a processing window") {
    val session = spark
    import session.implicits._

    val bronze = Seq(
      (1, Seq.empty[String], 2025, 1),
      (2, Seq("INVALID_TRIP_DURATION"), 2025, 1),
      (3, Seq.empty[String], 2025, 1),
      (4, Seq.empty[String], 2025, 1)
    ).toDF("VendorID", "validation_errors", "year", "month")

    val normalized = Seq(
      (1, Timestamp.valueOf("2025-01-01 08:00:00"), 2025, 1),
      (3, Timestamp.valueOf("2025-01-01 09:00:00"), 2025, 1),
      (4, Timestamp.valueOf("2025-01-01 10:00:00"), 2025, 1)
    ).toDF("vendorId", "pickupDatetime", "year", "month")

    val enriched = Seq(
      (1, "Upper East Side North", "Midtown Center", 2025, 1),
      (3, null, "Midtown Center", 2025, 1),
      (4, "Upper East Side North", null, 2025, 1)
    ).toDF("vendorId", "pickupZone", "dropoffZone", "year", "month")

    val anomalies = Seq(
      (1, Seq("VERY_HIGH_FARE"), true, 2025, 1)
    ).toDF("vendorId", "anomalyFlags", "isAnomalous", "year", "month")

    val result = DataQualityReportTransformation.build(
      bronzeDf = bronze,
      normalizedDf = normalized,
      enrichedDf = enriched,
      anomalyDf = anomalies,
      processingDate = "2026-03-10",
      year = 2025,
      month = 1
    )(session)

    assert(result.schema == DataQualityReportSchema.structType)
    assert(result.count() == 12L)

    val metrics = result
      .select("datasetName", "metricName", "metricValue")
      .collect()
      .map { row =>
        (row.getAs[String]("datasetName"), row.getAs[String]("metricName")) -> row.getAs[Double]("metricValue")
      }.toMap

    assert(metrics("bronze_validation" -> "totalRecords") == 4.0)
    assert(metrics("bronze_validation" -> "invalidRecords") == 1.0)
    assert(metrics("bronze_validation" -> "invalidRecordRatio") == 0.25)
    assert(metrics("silver_normalized_trips" -> "normalizedRecords") == 3.0)
    assert(metrics("silver_normalized_trips" -> "droppedInvalidRecords") == 1.0)
    assert(metrics("silver_normalized_trips" -> "normalizedRecordRatio") == 0.75)
    assert(metrics("silver_enriched_trips" -> "missingPickupZoneCount") == 1.0)
    assert(metrics("silver_enriched_trips" -> "missingDropoffZoneCount") == 1.0)
    assert(metrics("silver_enriched_trips" -> "missingPickupZoneRatio") == 1.0 / 3.0)
    assert(metrics("silver_enriched_trips" -> "missingDropoffZoneRatio") == 1.0 / 3.0)
    assert(metrics("gold_trip_anomalies" -> "anomalousTripsCount") == 1.0)
    assert(metrics("gold_trip_anomalies" -> "anomalousTripsRatio") == 1.0 / 3.0)
  }
}
