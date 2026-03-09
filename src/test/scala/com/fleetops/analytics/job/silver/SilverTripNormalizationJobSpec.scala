package com.fleetops.analytics.job.silver

import com.fleetops.analytics.common.ops.DataFrameOps._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class SilverTripNormalizationJobSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("SilverTripNormalizationJobSpec")
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

  test("normalize maps bronze schema, filters invalid records, and derives tripDurationSeconds") {
    val session = spark
    import session.implicits._

    val bronze = Seq(
      (1, Timestamp.valueOf("2025-01-01 10:00:00"), Timestamp.valueOf("2025-01-01 10:30:00"), 100, 101, 2L, 4.2, 12.0, 2.0, 15.5, 1L, Seq.empty[String], 2025, 1, "src1", Timestamp.valueOf("2025-01-02 00:00:00")),
      (2, Timestamp.valueOf("2025-01-01 11:00:00"), Timestamp.valueOf("2025-01-01 10:55:00"), 102, 103, 1L, 1.0, 9.0, 1.0, 10.0, 2L, Seq("INVALID_TRIP_DURATION"), 2025, 1, "src2", Timestamp.valueOf("2025-01-02 00:00:00"))
    ).toDF(
      "VendorID",
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "PULocationID",
      "DOLocationID",
      "passenger_count",
      "trip_distance",
      "fare_amount",
      "tip_amount",
      "total_amount",
      "payment_type",
      "validation_errors",
      "year",
      "month",
      "source_file",
      "ingestion_timestamp"
    )

    val result = bronze.normalize

    val columns = result.columns.toSeq
    assert(columns == Seq(
      "vendorId",
      "pickupDatetime",
      "dropoffDatetime",
      "pickupLocationId",
      "dropoffLocationId",
      "passengerCount",
      "tripDistance",
      "fareAmount",
      "tipAmount",
      "totalAmount",
      "paymentType",
      "tripDurationSeconds",
      "year",
      "month"
    ))

    val rows = result.collect()
    assert(rows.length == 1)

    val row = rows.head
    assert(row.getAs[Int]("vendorId") == 1)
    assert(row.getAs[Int]("pickupLocationId") == 100)
    assert(row.getAs[Int]("dropoffLocationId") == 101)
    assert(row.getAs[Long]("passengerCount") == 2L)
    assert(row.getAs[Double]("tripDistance") == 4.2)
    assert(row.getAs[Double]("fareAmount") == 12.0)
    assert(row.getAs[Double]("tipAmount") == 2.0)
    assert(row.getAs[Double]("totalAmount") == 15.5)
    assert(row.getAs[Long]("paymentType") == 1L)
    assert(row.getAs[Long]("tripDurationSeconds") == 1800L)
    assert(row.getAs[Int]("year") == 2025)
    assert(row.getAs[Int]("month") == 1)
  }
}
