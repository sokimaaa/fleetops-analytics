package com.fleetops.analytics.domain.schema

import org.apache.spark.sql.types._

object DataQualityReportSchema {
  val structType: StructType = StructType(
    Seq(
      StructField("processingDate", DateType, nullable = false),
      StructField("year", IntegerType, nullable = false),
      StructField("month", IntegerType, nullable = false),
      StructField("datasetName", StringType, nullable = false),
      StructField("metricName", StringType, nullable = false),
      StructField("metricValue", DoubleType, nullable = false)
    )
  )
}
