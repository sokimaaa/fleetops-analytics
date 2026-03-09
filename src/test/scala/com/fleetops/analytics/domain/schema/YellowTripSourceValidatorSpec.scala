package com.fleetops.analytics.domain.schema

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class YellowTripSourceValidatorSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("TripRecordValidatorSpec")
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

  test("withValidationErrors attaches validation_errors with expected rule violations") {
    val session = spark
    import session.implicits._

    val records = Seq(
      (1, Timestamp.valueOf("2025-01-01 10:00:00"), Timestamp.valueOf("2025-01-01 10:30:00"), 1.2, 10.0),
      (2, Timestamp.valueOf("2025-01-01 12:00:00"), Timestamp.valueOf("2025-01-01 11:00:00"), 0.8, 15.0),
      (3, Timestamp.valueOf("2025-01-01 09:00:00"), Timestamp.valueOf("2025-01-01 10:00:00"), -2.0, -1.0),
      (4, Timestamp.valueOf("2025-01-01 09:00:00"), Timestamp.valueOf("2025-01-01 09:10:00"), Double.NaN, Double.NaN)
    ).toDF("id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount")

    val validated = YellowTripSourceValidator.withValidationErrors(records)

    val errorsById = validated
      .select("id", YellowTripSourceValidator.ValidationErrorsColumn)
      .collect()
      .map(row => row.getInt(0) -> row.getSeq[String](1))
      .toMap

    assert(errorsById(1).isEmpty)
    assert(errorsById(2) == Seq("INVALID_TRIP_DURATION"))
    assert(errorsById(3) == Seq("NEGATIVE_DISTANCE", "INVALID_FARE"))
    assert(errorsById(4) == Seq("NEGATIVE_DISTANCE", "INVALID_FARE"))
  }

  test("invalidRecords returns only rows with empty validation_errors") {
    val session = spark
    import session.implicits._

    val records = Seq(
      (1, Timestamp.valueOf("2025-01-01 10:00:00"), Timestamp.valueOf("2025-01-01 10:30:00"), 1.0, 10.0),
      (2, Timestamp.valueOf("2025-01-01 10:30:00"), Timestamp.valueOf("2025-01-01 10:20:00"), 1.0, 10.0),
      (3, Timestamp.valueOf("2025-01-01 10:00:00"), Timestamp.valueOf("2025-01-01 10:30:00"), 0.0, 10.0)
    ).toDF("id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount")

    val validIds = YellowTripSourceValidator
      .validRecords(records)
      .select("id")
      .as[Int]
      .collect()
      .toSet

    assert(validIds == Set(1, 3))
  }
}
