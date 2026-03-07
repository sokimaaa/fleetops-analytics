package com.fleetops.analytics.common

import com.fleetops.analytics.config.ConfigLoader
import org.apache.spark.sql.SparkSession

object JobRunner extends TimedLogging {
  def run(job: SparkJob, args: Array[String]): Unit = {
    val jobName = getJobName(job)
    timed(s"Job $jobName") {
      var spark: Option[SparkSession] = None
      logInfo(s"Job started: $jobName")

      try {
        val appConfig = ConfigLoader.load()
        spark = Some(SparkSessionFactory.create(appConfig))
        spark.foreach(s => job.run(s, appConfig, args))
      } finally {
        logInfo(s"Stopping Spark session for job: $jobName")
        spark.foreach(_.stop())
      }
    }
  }

  private def getJobName(job: SparkJob): String = job.getClass.getSimpleName.stripSuffix("$")
}
