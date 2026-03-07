package com.fleetops.analytics.common

import com.fleetops.analytics.config.ConfigLoader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit

object JobRunner extends Logging {
  def run(job: SparkJob, args: Array[String]): Unit = {
    val startedAtNanos = System.nanoTime()
    var spark: Option[SparkSession] = None
    val jobName = calculateJobName(job)
    logInfo(s"Job started: $jobName")

    try {
      val appConfig = ConfigLoader.load()
      spark = Some(SparkSessionFactory.create(appConfig))

      spark.foreach(s => job.run(s, appConfig, args))
      val elapsedMs = nanosToMillis(System.nanoTime() - startedAtNanos)
      logInfo(s"Job completed: $jobName in ${elapsedMs}ms")
    } catch {
      case throwable: Throwable =>
        val elapsedMs = nanosToMillis(System.nanoTime() - startedAtNanos)
        logError(s"Job failed: $jobName after ${elapsedMs}ms", throwable)
        throw throwable
    } finally {
      logInfo(s"Stopping Spark session for job: $jobName")
      spark.foreach(_.stop())
    }
  }

  private def nanosToMillis(nanos: Long): Long = TimeUnit.NANOSECONDS.toMillis(nanos)

  private def calculateJobName(job: SparkJob): String = job.getClass.getSimpleName.stripSuffix("$")
}
