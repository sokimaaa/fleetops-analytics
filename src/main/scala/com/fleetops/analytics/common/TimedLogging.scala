package com.fleetops.analytics.common

import org.apache.spark.internal.Logging

import java.util.concurrent.TimeUnit

trait TimedLogging extends Logging {

  protected final def timed[A](operationName: String)(operation: => A): A = {
    val startedAtNanos = System.nanoTime()

    try {
      val result = operation
      logInfo(s"$operationName completed in ${nanosToMillis(System.nanoTime() - startedAtNanos)}ms")
      result
    } catch {
      case throwable: Throwable =>
        logError(s"$operationName failed after ${nanosToMillis(System.nanoTime() - startedAtNanos)}ms", throwable)
        throw throwable
    }
  }

  private def nanosToMillis(nanos: Long): Long = TimeUnit.NANOSECONDS.toMillis(nanos)
}
