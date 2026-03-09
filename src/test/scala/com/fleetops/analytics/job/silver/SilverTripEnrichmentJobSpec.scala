package com.fleetops.analytics.job.silver

import com.fleetops.analytics.transformation.ops.DataFrameOps._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class SilverTripEnrichmentJobSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("SilverTripEnrichmentJobSpec")
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

  test("withTaxiZones resolves pickup/dropoff zone metadata with broadcast joins") {
    val session = spark
    import session.implicits._

    val normalizedTrips = Seq(
      (1, Timestamp.valueOf("2025-01-01 10:00:00"), Timestamp.valueOf("2025-01-01 10:20:00"), 236, 161, 1L, 3.0, 10.0, 2.0, 13.0, 1L, 1200L, 2025, 1)
    ).toDF(
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
    )

    val zoneLookup = Seq(
      (236, "Manhattan", "Upper East Side North", "Yellow Zone"),
      (161, "Manhattan", "Midtown Center", "Yellow Zone")
    ).toDF("LocationID", "Borough", "Zone", "service_zone")

    val enriched = normalizedTrips.withTaxiZones(zoneLookup)
    val row = enriched.head()

    assert(row.getAs[String]("pickupZone") == "Upper East Side North")
    assert(row.getAs[String]("pickupBorough") == "Manhattan")
    assert(row.getAs[String]("dropoffZone") == "Midtown Center")
    assert(row.getAs[String]("dropoffBorough") == "Manhattan")

    val plan = enriched.queryExecution.executedPlan.toString()
    assert(plan.contains("Broadcast"))
  }
}
