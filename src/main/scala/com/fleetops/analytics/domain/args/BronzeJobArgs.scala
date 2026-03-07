package com.fleetops.analytics.domain.args

import java.nio.file.{Path, Paths}
import java.time.YearMonth

final case class BronzeJobArgs(inputPath: String, year: Int, month: Int)

object BronzeJobArgs {
  private[this] val RequiredArgs = Set("--input-path", "--year", "--month")

  def apply(args: Array[String]): BronzeJobArgs = parseArgs(args)

  private def parseArgs(args: Array[String]): BronzeJobArgs = {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException(
        "Invalid arguments format. Expected key-value pairs: --input-path <path> --year <YYYY> --month <MM>"
      )
    }

    val parsed = args.grouped(2).map {
      case Array(key, value) if RequiredArgs.contains(key) => key -> value
    }.toMap

    val inputPath: Path = parsed.get("--input-path").map(validatePath).getOrElse(missingArg("--input-path"))
    val yearMonth = parsed.get("--year")
      .map(_.toInt)
      .zip(parsed.get("--month")
        .map(_.toInt)
      )
      .map(validateYearMonth)
      .getOrElse(missingArg("--year, --month"))

    BronzeJobArgs(
      inputPath = inputPath.toString,
      year = yearMonth.getYear,
      month = yearMonth.getMonthValue
    )
  }

  private def validateYearMonth(yearMonth: (Int, Int)): YearMonth = try {
    YearMonth.of(yearMonth._1, yearMonth._2)
  } catch {
    case _: Exception =>
      throw new IllegalArgumentException(s"Invalid --month or --year args")
  }

  private def validatePath(path: String): Path = try {
    Paths.get(path)
  } catch {
    case _: Exception =>
      throw new IllegalArgumentException(s"Invalid --input-path")
  }

  private[this] def missingArg(argName: String): Nothing =
    throw new IllegalArgumentException(s"Missing required argument: $argName")
}
