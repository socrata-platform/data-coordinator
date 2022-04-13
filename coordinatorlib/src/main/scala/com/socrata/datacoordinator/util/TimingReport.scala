package com.socrata.datacoordinator.util

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.thirdparty.metrics.Metrics
import org.slf4j.{MDC, Logger}

trait TimingReport {
  def apply[T](name: String, kv: (String, Any)*)(f: => T): T

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

  protected def infoReport[T](name: String, kv: (String, Any)*)(f: => T): T = {
    time(f) { (elapsed: Long) =>
      if(logger.isInfoEnabled) {
        logger.info("{}: {}ms; {}", name, elapsed.toString, content(kv: _*))
      }
    }
  }

  protected def debugReport[T](name: String, kv: (String, Any)*)(f: => T): T = {
    time(f) { (elapsed: Long) =>
      if(logger.isDebugEnabled) {
        logger.debug("{}: {}ms; {}", name, elapsed.toString, content(kv: _*))
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

  abstract override def apply[T](name: String, kv: (String, Any)*)(f: => T): T = {
    contextLocal.set(name :: context)
    try {
      super.apply(context.reverse.mkString("/"), kv: _*)(f)
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
  abstract override def apply[T](name: String, kv: (String, Any)*)(f: => T): T = {
    val timer = metrics.timer(name)
    timer.time {
      super.apply(name, kv: _*)(f)
    }
  }
}

class LoggedTimingReport(log: Logger) extends LogTimingReport {
  val logger = log

  def apply[T](name: String, kv: (String, Any)*)(f: => T): T = infoReport(name, kv: _*)(f)
}

class DebugLoggedTimingReport(log: Logger) extends LogTimingReport {
  val logger = log

  def apply[T](name: String, kv: (String, Any)*)(f: => T): T = debugReport(name, kv: _*)(f)

  override def info[T](name: String, kv: (String, Any)*)(f: => T): T = infoReport(name, kv: _*)(f)
}

/**
 * Put keys with prefix - tag: in MDC so that enclosing log statements can be tagged using %X{name} in log config.
 * Example:
 *   key -> tag:job-id
 *   log4j.appender.console.props.layout.props.ConversionPattern -> "%X{job-id})"
 */
trait TaggableTimingReport extends TimingReport with Metrics {

  private val Tag = "^tag:(.+)$".r

  abstract override def apply[T](name: String, kv: (String, Any)*)(f: => T): T = {

    val (regKv, tagKv) = kv.partition { case (tag, value) => Tag.findFirstIn(tag).isEmpty }

    val tagK = tagKv.collect { case (Tag(tag), value) =>
      MDC.put(tag, value.toString)
      tag
    }

    try {
      super.apply(name, regKv: _*)(f)
    } finally {
      tagK.foreach(MDC.remove(_))
    }
  }
}

object NoopTimingReport extends TransferrableContextTimingReport {
  def apply[T](name: String, kv: (String, Any)*)(f: => T): T = f

  type Context = Unit
  def context = ()
  def withContext[T](ctx: Unit)(f: => T) = f
}

