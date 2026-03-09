package com.fleetops.analytics.common.io

import org.apache.spark.sql.{DataFrame, SaveMode}

object PartitionedParquetWriter extends PartitionedWriter {
  override def write(dataFrame: DataFrame)(outputPath: String, partitionColumns: Seq[String]): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionColumns: _*)
      .parquet(outputPath)
  }
}
