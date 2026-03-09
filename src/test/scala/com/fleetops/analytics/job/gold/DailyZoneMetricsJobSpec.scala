package com.fleetops.analytics.job.gold

import com.fleetops.analytics.common.ops.DataFrameOps._
import com.fleetops.analytics.domain.schema.DailyZoneMetricsSchema
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class DailyZoneMetricsJobSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("DailyZoneMetricsJobSpec")
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

  test("toDailyZoneMetrics aggregates daily pickup zone KPIs and derives partition fields") {
    val session = spark
    import session.implicits._

    val enrichedTrips = Seq(
      (Timestamp.valueOf("2025-01-01 08:00:00"), 236, "Manhattan", "Upper East Side North", 3.0, 10.0, 2.0, 13.0, 1200L),
      (Timestamp.valueOf("2025-01-01 09:30:00"), 236, "Manhattan", "Upper East Side North", 5.0, 14.0, 1.0, 16.0, 1800L),
      (Timestamp.valueOf("2025-01-02 11:00:00"), 161, "Manhattan", "Midtown Center", 2.5, 9.0, 0.0, 10.0, 900L)
    ).toDF(
      "pickupDatetime",
      "pickupLocationId",
      "pickupBorough",
      "pickupZone",
      "tripDistance",
      "fareAmount",
      "tipAmount",
      "totalAmount",
      "tripDurationSeconds"
    )

    val result = enrichedTrips.toDailyZoneMetrics

    assert(result.schema == DailyZoneMetricsSchema.structType)
    assert(result.count() == 2L)

    val jan1 = result
      .filter($"tripDate".cast("string") === "2025-01-01" && $"pickupLocationId" === 236)
      .select(
        $"tripsCount",
        $"totalTripDistance",
        $"avgTripDistance",
        $"totalFareAmount",
        $"avgFareAmount",
        $"totalTipAmount",
        $"totalRevenue",
        $"avgTripDurationSeconds",
        $"year",
        $"month"
      )
      .head()

    assert(jan1.getAs[Long]("tripsCount") == 2L)
    assert(jan1.getAs[Double]("totalTripDistance") == 8.0)
    assert(jan1.getAs[Double]("avgTripDistance") == 4.0)
    assert(jan1.getAs[Double]("totalFareAmount") == 24.0)
    assert(jan1.getAs[Double]("avgFareAmount") == 12.0)
    assert(jan1.getAs[Double]("totalTipAmount") == 3.0)
    assert(jan1.getAs[Double]("totalRevenue") == 29.0)
    assert(jan1.getAs[Double]("avgTripDurationSeconds") == 1500.0)
    assert(jan1.getAs[Int]("year") == 2025)
    assert(jan1.getAs[Int]("month") == 1)
  }
}
