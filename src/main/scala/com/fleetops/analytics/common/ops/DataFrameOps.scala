package com.fleetops.analytics.common.ops

import com.fleetops.analytics.common.ProcessingWindow
import com.fleetops.analytics.config.AppConfig
import com.fleetops.analytics.domain.schema.YellowTripSourceValidator
import com.fleetops.analytics.transformation._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object DataFrameOps {
  implicit class ProcessingWindowOps(private val value: DataFrame) extends AnyVal {
    def forProcessingWindow(window: ProcessingWindow): DataFrame =
      value.filter(col("year") === window.year && col("month") === window.month)
  }

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

  implicit class TripMetricsOps(private val value: DataFrame) extends AnyVal {
    def toDailyZoneMetrics: DataFrame = DailyZoneMetricsTransformation.aggregate(value)

    def toHourlyHotspotMetrics: DataFrame = HourlyHotspotMetricsTransformation.aggregate(value)
  }

  implicit class AnomalyTripOps(private val value: DataFrame)(implicit config: AppConfig) {
    def withAnomalyRules: DataFrame = AnomalyRuleEnricherTransformation.withAnomalyRule(value)

    def filterAnomalous: DataFrame = AnomalyRuleEnricherTransformation.filterAnomalous(value)
  }
}
