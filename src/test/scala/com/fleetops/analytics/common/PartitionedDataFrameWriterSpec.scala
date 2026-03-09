package com.fleetops.analytics.common

import com.fleetops.analytics.common.io.PartitionedParquetWriter
import com.fleetops.analytics.common.ops.DataFrameOps._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class PartitionedDataFrameWriterSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("PartitionedDataFrameWriterSpec")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  test("write overwrites only the target partition on rerun") {
    val session = spark
    import session.implicits._

    val outputPath = Files.createTempDirectory("partition-writer-spec").toAbsolutePath.toString

    val initialDf = Seq(
      ("jan-original", 2025, 1),
      ("feb-stable", 2025, 2)
    ).toDF("value", "year", "month")

    PartitionedParquetWriter.write(initialDf)(outputPath, Seq("year", "month"))

    val rerunDf = Seq(
      ("jan-recomputed", 2025, 1)
    ).toDF("value", "year", "month")

    PartitionedParquetWriter.write(rerunDf)(outputPath, Seq("year", "month"))

    val result = session.read.parquet(outputPath)

    assert(result.filter($"year" === 2025 && $"month" === 1).select("value").as[String].collect().toSeq == Seq("jan-recomputed"))
    assert(result.filter($"year" === 2025 && $"month" === 2).select("value").as[String].collect().toSeq == Seq("feb-stable"))
    assert(result.count() == 2L)
  }

  test("forProcessingWindow keeps only the configured year and month") {
    val session = spark
    import session.implicits._

    val dataFrame = Seq(
      ("jan", 2025, 1),
      ("feb", 2025, 2),
      ("prev-year", 2024, 1)
    ).toDF("value", "year", "month")

    val result = dataFrame.forProcessingWindow(ProcessingWindow(2025, 1))

    assert(result.select("value").as[String].collect().toSeq == Seq("jan"))
  }
}
