package com.fleetops.analytics.common

import com.fleetops.analytics.config.{AppConfig, ConfigLoader}
import org.apache.spark.sql.SparkSession

object JobRunner extends TimedLogging {
  def run(job: SparkJob, args: Array[String]): Unit = {
    val jobName = getJobName(job)
    timed(s"Job $jobName") {
      var spark: Option[SparkSession] = None
      logInfo(s"Job started: $jobName")

      try {
        implicit val appConfig: AppConfig = ConfigLoader.load()
        spark = Some(SparkSessionFactory.create(appConfig))
        spark.foreach { implicit ss =>
          job.run(args)
        }
      } finally {
        logInfo(s"Stopping Spark session for job: $jobName")
        spark.foreach(_.stop())
      }
    }
  }

  private def getJobName(job: SparkJob): String = job.getClass.getSimpleName.stripSuffix("$")
}
