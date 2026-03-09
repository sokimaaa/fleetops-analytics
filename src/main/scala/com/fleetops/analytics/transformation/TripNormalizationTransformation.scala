package com.fleetops.analytics.transformation

import com.fleetops.analytics.transformation.ops.DataFrameOps._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, timestamp_diff}
import org.apache.spark.sql.types.DataTypes

object TripNormalizationTransformation {
  def normalize(bronzeDf: DataFrame): DataFrame = {
    bronzeDf
      .filterValidRecords
      .select(
        col("VendorID").cast(DataTypes.IntegerType).as("vendorId"),
        col("tpep_pickup_datetime").cast(DataTypes.TimestampNTZType).as("pickupDatetime"),
        col("tpep_dropoff_datetime").cast(DataTypes.TimestampNTZType).as("dropoffDatetime"),
        col("PULocationID").cast(DataTypes.IntegerType).as("pickupLocationId"),
        col("DOLocationID").cast(DataTypes.IntegerType).as("dropoffLocationId"),
        col("passenger_count").cast(DataTypes.LongType).as("passengerCount"),
        col("trip_distance").cast(DataTypes.DoubleType).as("tripDistance"),
        col("fare_amount").cast(DataTypes.DoubleType).as("fareAmount"),
        col("tip_amount").cast(DataTypes.DoubleType).as("tipAmount"),
        col("total_amount").cast(DataTypes.DoubleType).as("totalAmount"),
        col("payment_type").cast(DataTypes.LongType).as("paymentType"),
        timestamp_diff(
          "SECOND",
          col("tpep_pickup_datetime"),
          col("tpep_dropoff_datetime")
        ).cast(DataTypes.LongType).as("tripDurationSeconds"),
        col("year"),
        col("month")
      )
  }
}
