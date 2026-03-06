package com.socrata.datacoordinator.util

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.thirdparty.metrics.Metrics
import org.slf4j.{MDC, Logger}

trait TimingReport {
  final def apply[T](name: String, kv: (String, Any)*)(f: => T): T =
    withWarnThreshold(name, Long.MaxValue, kv: _*)(f)

  def withWarnThreshold[T](name: String, ms: Long, kv: (String, Any)*)(f: => T): T

  def info[T](name: String, kv: (String, Any)*)(f: => T): T = apply(name, kv: _*)(f)

  protected def content(kv: (String, Any)*): String = {
    JsonUtil.renderJson(kv.map { case (k,v) => (k, String.valueOf(v)) })
  }

  protected def time[T](f: => T)(report: (Long) => Unit): T = {
    val start = System.nanoTime()
    try {
      f
    } finally {
      val end = System.nanoTime()
      val timeInMs = (end - start) / 1000000
      report(timeInMs)
    }
  }
}

trait LogTimingReport extends TimingReport {
  val logger: Logger

  protected def infoReport[T](name: String, threshold: Long, kv: (String, Any)*)(f: => T): T = {
    time(f) { (elapsed: Long) =>
      if(elapsed > threshold) {
        if(logger.isWarnEnabled) {
          logger.warn("{}: {}ms; {}", name, elapsed.toString, content(kv: _*), new Exception)
        }
      } else {
        if(logger.isInfoEnabled) {
          logger.info("{}: {}ms; {}", name, elapsed.toString, content(kv: _*))
        }
      }
    }
  }

  protected def debugReport[T](name: String, threshold: Long, kv: (String, Any)*)(f: => T): T = {
    time(f) { (elapsed: Long) =>
      if(elapsed > threshold) {
        if(logger.isWarnEnabled) {
          logger.warn("{}: {}ms; {}", name, elapsed.toString, content(kv: _*), new Exception)
        }
      } else {
        if(logger.isDebugEnabled) {
          logger.debug("{}: {}ms; {}", name, elapsed.toString, content(kv: _*))
        }
      }
    }
  }
}

trait TransferrableContextTimingReport extends TimingReport {
  // for use when getting a worker from a thread pool
  type Context
  def context: Context
  def withContext[T](context: Context)(f: => T): T
}

trait StackedTimingReport extends TimingReport with TransferrableContextTimingReport {
  private val contextLocal = new ThreadLocal[List[String]] {
    override def initialValue = Nil
  }

  type Context = List[String]

  def context: List[String] = contextLocal.get

  abstract override def withWarnThreshold[T](name: String, ms: Long, kv: (String, Any)*)(f: => T): T = {
    contextLocal.set(name :: context)
    try {
      super.withWarnThreshold(context.reverse.mkString("/"), ms, kv: _*)(f)
    } finally {
      contextLocal.set(context.tail)
    }
  }

  def withContext[T](context: Context)(f: => T): T = {
    val oldContext = contextLocal.get
    contextLocal.set(context)
    try {
      f
    } finally {
      contextLocal.set(oldContext)
    }
  }
}

trait MetricsTimingReport extends TimingReport with Metrics {
  abstract override def withWarnThreshold[T](name: String, ms: Long, kv: (String, Any)*)(f: => T): T = {
    val timer = metrics.timer(name)
    timer.time {
      super.withWarnThreshold(name, ms, kv: _*)(f)
    }
  }
}

class LoggedTimingReport(log: Logger) extends LogTimingReport {
  val logger = log

  def withWarnThreshold[T](name: String, ms: Long, kv: (String, Any)*)(f: => T): T =
    infoReport(name, ms, kv: _*)(f)
}

class DebugLoggedTimingReport(log: Logger) extends LogTimingReport {
  val logger = log

  def withWarnThreshold[T](name: String, ms: Long, kv: (String, Any)*)(f: => T): T =
    debugReport(name, ms, kv: _*)(f)

  override def info[T](name: String, kv: (String, Any)*)(f: => T): T =
    infoReport(name, Long.MaxValue, kv: _*)(f)
}

/**
 * Put keys with prefix - tag: in MDC so that enclosing log statements can be tagged using %X{name} in log config.
 * Example:
 *   key -> tag:job-id
 *   log4j.appender.console.props.layout.props.ConversionPattern -> "%X{job-id})"
 */
trait TaggableTimingReport extends TimingReport with Metrics {

  private val Tag = "^tag:(.+)$".r

  abstract override def withWarnThreshold[T](name: String, ms: Long, kv: (String, Any)*)(f: => T): T = {

    val (regKv, tagKv) = kv.partition { case (tag, value) => Tag.findFirstIn(tag).isEmpty }

    val tagK = tagKv.collect { case (Tag(tag), value) =>
      MDC.put(tag, value.toString)
      tag
    }

    try {
      super.withWarnThreshold(name, ms, regKv: _*)(f)
    } finally {
      tagK.foreach(MDC.remove(_))
    }
  }
}

object NoopTimingReport extends TransferrableContextTimingReport {
  def withWarnThreshold[T](name: String, ms: Long, kv: (String, Any)*)(f: => T): T = f

  type Context = Unit
  def context = ()
  def withContext[T](ctx: Unit)(f: => T) = f
}

