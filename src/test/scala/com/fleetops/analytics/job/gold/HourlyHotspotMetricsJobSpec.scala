package com.fleetops.analytics.job.gold

import com.fleetops.analytics.domain.schema.HourlyHotspotMetricsSchema
import com.fleetops.analytics.transformation.ops.DataFrameOps._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class HourlyHotspotMetricsJobSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("HourlyHotspotMetricsJobSpec")
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

  test("toHourlyHotspotMetrics aggregates hourly pickup hotspot KPIs and derives partition fields") {
    val session = spark
    import session.implicits._

    val enrichedTrips = Seq(
      (Timestamp.valueOf("2025-01-01 08:05:00"), 236, "Manhattan", "Upper East Side North", 13.0),
      (Timestamp.valueOf("2025-01-01 08:55:00"), 236, "Manhattan", "Upper East Side North", 17.0),
      (Timestamp.valueOf("2025-01-01 09:10:00"), 236, "Manhattan", "Upper East Side North", 19.0),
      (Timestamp.valueOf("2025-01-02 11:00:00"), 161, "Manhattan", "Midtown Center", 10.0)
    ).toDF(
      "pickupDatetime",
      "pickupLocationId",
      "pickupBorough",
      "pickupZone",
      "totalAmount"
    )

    val result = enrichedTrips.toHourlyHotspotMetrics

    assert(result.schema == HourlyHotspotMetricsSchema.structType)
    assert(result.count() == 3L)

    val jan1Hour8 = result
      .filter(
        $"tripDate".cast("string") === "2025-01-01" &&
          $"pickupHour" === 8 &&
          $"pickupLocationId" === 236
      )
      .select(
        $"tripsCount",
        $"totalRevenue",
        $"avgRevenuePerTrip",
        $"year",
        $"month"
      )
      .head()

    assert(jan1Hour8.getAs[Long]("tripsCount") == 2L)
    assert(jan1Hour8.getAs[Double]("totalRevenue") == 30.0)
    assert(jan1Hour8.getAs[Double]("avgRevenuePerTrip") == 15.0)
    assert(jan1Hour8.getAs[Int]("year") == 2025)
    assert(jan1Hour8.getAs[Int]("month") == 1)
  }
}
