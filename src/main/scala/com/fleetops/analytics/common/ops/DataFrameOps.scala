package com.fleetops.analytics.common.ops

import com.fleetops.analytics.domain.schema.YellowTripSourceValidator
import org.apache.spark.sql.DataFrame

object DataFrameOps {

  implicit class YellowTripSourceOps(private val value: DataFrame) {
    def withValidationErrors(): DataFrame = YellowTripSourceValidator.withValidationErrors(value)

    def filterInvalidRecords(): DataFrame = YellowTripSourceValidator.invalidRecords(value)
  }
}
