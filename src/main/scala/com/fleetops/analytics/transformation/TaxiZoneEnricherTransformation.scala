package com.fleetops.analytics.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.types.DataTypes

object TaxiZoneEnricherTransformation {

  def enrichWithZones(normalizedTripsDf: DataFrame, rawZoneLookupDf: DataFrame): DataFrame = {
    val zones = rawZoneLookupDf
      .select(
        col("LocationID").cast(DataTypes.IntegerType).as("locationId"),
        col("Borough").as("borough"),
        col("Zone").as("zone"),
      )

    val pickupZones = broadcast(
      zones.select(
        col("locationId").as("pickupLocationId"),
        col("borough").as("pickupBorough"),
        col("zone").as("pickupZone"),
      )
    )

    val dropoffZones = broadcast(
      zones.select(
        col("locationId").as("dropoffLocationId"),
        col("borough").as("dropoffBorough"),
        col("zone").as("dropoffZone"),
      )
    )

    normalizedTripsDf
      .join(pickupZones, "pickupLocationId", "left")
      .join(dropoffZones, "dropoffLocationId", "left")
  }
}
