package com.fleetops.analytics.common.io

import org.apache.spark.sql.DataFrame

trait PartitionedWriter {
  def write(dataFrame: DataFrame)(outputPath: String, partitionColumns: Seq[String]): Unit
}
