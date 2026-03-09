package com.fleetops.analytics.common

import com.fleetops.analytics.config.DatasetConfig

final case class ProcessingWindow(year: Int, month: Int)

object ProcessingWindow {
  def from(datasetConfig: DatasetConfig): ProcessingWindow =
    ProcessingWindow(datasetConfig.year, datasetConfig.month)
}
