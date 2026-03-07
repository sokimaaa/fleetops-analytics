package com.fleetops.analytics.domain.args

import org.scalatest.funsuite.AnyFunSuite

class BronzeJobArgsSpec extends AnyFunSuite {

  test("apply parses valid job arguments") {
    val args = Array(
      "--input-path", "data/raw/2025/yellow_tripdata_2025-01.parquet",
      "--year", "2025",
      "--month", "1"
    )

    val parsed = BronzeJobArgs(args)

    assert(parsed.inputPath == "data/raw/2025/yellow_tripdata_2025-01.parquet")
    assert(parsed.year == 2025)
    assert(parsed.month == 1)
  }

  test("apply throws when arguments are not key-value pairs") {
    val args = Array("--input-path", "data/raw/file.parquet", "--year")

    val error = intercept[IllegalArgumentException] {
      BronzeJobArgs(args)
    }

    assert(error.getMessage.contains("Invalid arguments format"))
  }

  test("apply throws when required month argument is missing") {
    val args = Array(
      "--input-path", "data/raw/file.parquet",
      "--year", "2025"
    )

    val error = intercept[IllegalArgumentException] {
      BronzeJobArgs(args)
    }

    assert(error.getMessage.contains("Missing required argument: --year, --month"))
  }

  test("apply throws when year-month pair is invalid") {
    val args = Array(
      "--input-path", "data/raw/file.parquet",
      "--year", "2025",
      "--month", "13"
    )

    val error = intercept[IllegalArgumentException] {
      BronzeJobArgs(args)
    }

    assert(error.getMessage.contains("Invalid --month or --year args"))
  }

  test("apply throws when input path is invalid") {
    val args = Array(
      "--input-path", "bad\u0000path",
      "--year", "2025",
      "--month", "1"
    )

    val error = intercept[IllegalArgumentException] {
      BronzeJobArgs(args)
    }

    assert(error.getMessage.contains("Invalid --input-path"))
  }
}
