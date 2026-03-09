package com.fleetops.analytics.domain.schema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object YellowTripSourceValidator {
  private val InvalidTripDurationError = "INVALID_TRIP_DURATION"

  private val InvalidTripDurationExpr = when(
    col("tpep_pickup_datetime").isNotNull
      && col("tpep_dropoff_datetime").isNotNull
      && !(col("tpep_pickup_datetime") < col("tpep_dropoff_datetime")),
    lit(InvalidTripDurationError)
  )

  private val NegativeDistanceError = "NEGATIVE_DISTANCE"

  private val NegativeDistanceExpr = when(
    col("trip_distance").isNotNull
    && (col("trip_distance") < 0 || isnan(col("trip_distance"))),
    lit(NegativeDistanceError)
  )

  private val InvalidFareError = "INVALID_FARE"

  private val InvalidFareExpr = when(
    col("fare_amount").isNotNull
    && (col("fare_amount") < 0 || isnan(col("fare_amount"))),
    InvalidFareError
  )

  val ValidationErrorsColumn = "validation_errors"

  private val ValidationErrorsExpr = filter(
    array(
      InvalidTripDurationExpr,
      NegativeDistanceExpr,
      InvalidFareExpr
    ),
    _.isNotNull
  )

  def withValidationErrors(df: DataFrame): DataFrame = {
    df.withColumn(
      ValidationErrorsColumn,
      ValidationErrorsExpr
    )
  }

  def validRecords(df: DataFrame): DataFrame = {
    val validated = if (df.columns.contains(ValidationErrorsColumn)) df else withValidationErrors(df)
    validated.filter(size(col(ValidationErrorsColumn)) === 0)
  }
}
