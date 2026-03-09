package com.fleetops.analytics.domain.model

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

sealed trait AnomalyFlag {
  def column: Column = lit(code)

  def code: String
}

object AnomalyFlag {
  case object VeryLongDuration extends AnomalyFlag {
    override def code: String = "VeryLongDuration"
  }

  case object VeryHighFare extends AnomalyFlag {
    override def code: String = "VeryHighFare"
  }

  case object VeryHighTip extends AnomalyFlag {
    override def code: String = "VeryHighTip"
  }

  case object LowDistanceHighFare extends AnomalyFlag {
    override def code: String = "LowDistanceHighFare"
  }
}

