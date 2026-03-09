package com.fleetops.analytics.transformation

import com.fleetops.analytics.config.AppConfig
import com.fleetops.analytics.domain.model.AnomalyFlag
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AnomalyRuleEnricherTransformation {

  def withAnomalyRule(enrichedTripsDf: DataFrame)(implicit config: AppConfig): DataFrame = {
    enrichedTripsDf
      .withColumn(
        "anomalyFlags",
        filter(
          array(
            when(
              col("tripDurationSeconds") > config.anomaly.veryLongDurationSeconds,
              AnomalyFlag.VeryLongDuration.column
            ),
            when(
              col("fareAmount") > config.anomaly.veryHighFareAmount,
              AnomalyFlag.VeryHighFare.column
            ),
            when(
              col("tipAmount") > config.anomaly.veryHighTipAmount,
              AnomalyFlag.VeryHighTip.column
            ),
            when(
              col("tripDistance") < config.anomaly.lowDistanceKmThreshold
                && col("fareAmount") > config.anomaly.lowDistanceHighFareAmount,
              AnomalyFlag.LowDistanceHighFare.column
            )
          ),
          _.isNotNull
        )
      )
      .withColumn(
        "isAnomalous",
        size(col("anomalyFlags")) > 0
      )
  }

  def filterAnomalous(df: DataFrame)(implicit config: AppConfig): DataFrame = {
    val anomalyDf = if (df.schema.contains("isAnomalous")) df else withAnomalyRule(df)

    anomalyDf.filter(col("isAnomalous"))
  }
}
