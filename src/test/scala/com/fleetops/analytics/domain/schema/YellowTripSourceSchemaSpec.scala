package com.fleetops.analytics.domain.schema

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class YellowTripSourceSchemaSpec extends AnyFunSuite {

  test("requireMatch does not throw when schema matches expected contract") {
    val matchingSchema = YellowTripSourceSchema.structType

    YellowTripSourceSchema.requireMatch(matchingSchema)
    succeed
  }

  test("requireMatch throws when schema has fewer fields than expected") {
    val fewerFieldsSchema = StructType(YellowTripSourceSchema.structType.fields.dropRight(1))

    val error = intercept[IllegalArgumentException] {
      YellowTripSourceSchema.requireMatch(fewerFieldsSchema)
    }

    assert(error.getMessage.contains("does not match"))
  }

  test("requireMatch throws when schema has extra fields") {
    val extraFieldSchema = StructType(
      YellowTripSourceSchema.structType.fields :+
        StructField("unexpected_col", StringType, nullable = true)
    )

    val error = intercept[IllegalArgumentException] {
      YellowTripSourceSchema.requireMatch(extraFieldSchema)
    }

    assert(error.getMessage.contains("does not match"))
  }

  test("requireMatch throws when field name differs") {
    val expectedFields = YellowTripSourceSchema.structType.fields
    val firstField = expectedFields.head
    val renamedFirstField = firstField.copy(name = s"${firstField.name}_renamed")
    val renamedSchema = StructType(renamedFirstField +: expectedFields.tail)

    val error = intercept[IllegalArgumentException] {
      YellowTripSourceSchema.requireMatch(renamedSchema)
    }

    assert(error.getMessage.contains("does not match"))
  }

  test("requireMatch throws when field type differs") {
    val expectedFields = YellowTripSourceSchema.structType.fields
    val firstField = expectedFields.head
    val changedTypeField = firstField.copy(dataType = StringType)
    val changedTypeSchema = StructType(changedTypeField +: expectedFields.tail)

    val error = intercept[IllegalArgumentException] {
      YellowTripSourceSchema.requireMatch(changedTypeSchema)
    }

    assert(error.getMessage.contains("does not match"))
  }

  test("requireMatch returns cumulative error when multiple fields mismatch") {
    val expectedFields = YellowTripSourceSchema.structType.fields
    val renamedFirstField = expectedFields.head.copy(name = s"${expectedFields.head.name}_renamed")
    val changedSecondFieldType = expectedFields(1).copy(dataType = StringType)
    val schemaWithMultipleMismatches = StructType(
      renamedFirstField +: changedSecondFieldType +: expectedFields.drop(2)
    )

    val error = intercept[IllegalArgumentException] {
      YellowTripSourceSchema.requireMatch(schemaWithMultipleMismatches)
    }

    assert(error.getMessage.contains("does not match"))
  }
}
