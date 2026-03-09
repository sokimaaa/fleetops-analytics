package com.fleetops.analytics.transformation.ops

import com.fleetops.analytics.domain.schema.YellowTripSourceValidator
import com.fleetops.analytics.transformation.{TaxiZoneEnricherTransformation, TripNormalizationTransformation}
import org.apache.spark.sql.DataFrame

object DataFrameOps {

  implicit class YellowTripSourceOps(private val value: DataFrame) extends AnyVal {
    def withValidationErrors: DataFrame = YellowTripSourceValidator.withValidationErrors(value)

    def filterValidRecords: DataFrame = YellowTripSourceValidator.validRecords(value)
  }

  implicit class TripNormalizationOps(private val value: DataFrame) extends AnyVal {
    def normalize: DataFrame = TripNormalizationTransformation.normalize(value)
  }

  implicit class TripEnricherOps(private val value: DataFrame) extends AnyVal {
    def withTaxiZones(zoneLookup: DataFrame): DataFrame =
      TaxiZoneEnricherTransformation.enrichWithZones(value, zoneLookup)
  }
}
