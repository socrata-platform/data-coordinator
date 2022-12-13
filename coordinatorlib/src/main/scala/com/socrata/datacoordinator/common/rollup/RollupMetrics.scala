package com.socrata.datacoordinator.common.rollup


object RollupMetricEvent extends Enumeration {
  type RollupMetricEvent = Value
  val Write, Read = Value
}

import com.socrata.datacoordinator.common.rollup.RollupMetricEvent._
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

sealed case class RollupMetric(dataset: String, rollup: String, eventType: RollupMetricEvent, time: Duration, size: Long)

object RollupMetrics {
  private val logger = LoggerFactory.getLogger(RollupMetrics.getClass)

  def digest(metric: RollupMetric): Unit = {
    logger.info(metric.toString)
  }
}
