package com.fleetops.analytics.common.ops

import com.fleetops.analytics.common.io.PartitionedParquetWriter
import org.apache.spark.sql.DataFrame

object WriterOps {

  implicit class ParquetWriterOps(private val value: DataFrame) extends AnyVal {
    def writeParquet: (String, Seq[String]) => Unit = PartitionedParquetWriter.write(value)
  }
}
