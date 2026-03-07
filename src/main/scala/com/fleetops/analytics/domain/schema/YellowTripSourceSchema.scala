package com.fleetops.analytics.domain.schema

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

object YellowTripSourceSchema extends Logging {
  val structType: StructType = StructType(
    Seq(
      StructField("VendorID", IntegerType, nullable = true),
      StructField("tpep_pickup_datetime", TimestampNTZType, nullable = true),
      StructField("tpep_dropoff_datetime", TimestampNTZType, nullable = true),
      StructField("passenger_count", LongType, nullable = true),
      StructField("trip_distance", DoubleType, nullable = true),
      StructField("RatecodeID", LongType, nullable = true),
      StructField("store_and_fwd_flag", StringType, nullable = true),
      StructField("PULocationID", IntegerType, nullable = true),
      StructField("DOLocationID", IntegerType, nullable = true),
      StructField("payment_type", LongType, nullable = true),
      StructField("fare_amount", DoubleType, nullable = true),
      StructField("extra", DoubleType, nullable = true),
      StructField("mta_tax", DoubleType, nullable = true),
      StructField("tip_amount", DoubleType, nullable = true),
      StructField("tolls_amount", DoubleType, nullable = true),
      StructField("improvement_surcharge", DoubleType, nullable = true),
      StructField("total_amount", DoubleType, nullable = true),
      StructField("congestion_surcharge", DoubleType, nullable = true),
      StructField("Airport_fee", DoubleType, nullable = true),
      StructField("cbd_congestion_fee", DoubleType, nullable = true)
    )
  )

  def requireMatch(actualSchema: StructType): Unit = {
    val mismatches = findMismatches(actualSchema)

    if (mismatches.nonEmpty) {
      logInfo(s"Raw dataset schema does not match YellowTripSourceSchema. Found ${mismatches.size} mismatch(es).")
      mismatches.foreach(logError(_))
      throw new IllegalArgumentException(
        s"Raw dataset schema does not match YellowTripSourceSchema. Expected=$structType, actual=$actualSchema"
      )
    }
  }

  private def findMismatches(actualSchema: StructType): Seq[String] = {
    val actualFields = actualSchema.fields
    val expectedFields = structType.fields

    val sizeMismatch = if (actualFields.length != expectedFields.length) {
      Seq(
        s"Schema field count mismatch: expected=${expectedFields.length}, actual=${actualFields.length}"
      )
    } else {
      Seq.empty
    }

    val fieldMismatches = expectedFields
      .map(Option(_))
      .zipAll(actualFields.map(Option(_)), None, None)
      .flatMap {
        case (Some(expectedField), Some(actualField)) if expectedField.name == actualField.name && actualField.dataType == expectedField.dataType =>
          Seq.empty
        case (Some(expectedField), Some(actualField)) =>
          Seq(
            s"Field mismatch: expected=${formatField(expectedField)}, actual=${formatField(actualField)}"
          )
        case (Some(expectedField), None) =>
          Seq(
            s"Missing field: expected=${formatField(expectedField)}"
          )
        case (None, Some(actualField)) =>
          Seq(
            s"Extra field: actual=${formatField(actualField)}, expected=None"
          )
      }

    sizeMismatch ++ fieldMismatches
  }

  private def formatField(field: StructField): String =
    s"(name=${field.name}, type=${field.dataType.simpleString}, nullable=${field.nullable})"
}
