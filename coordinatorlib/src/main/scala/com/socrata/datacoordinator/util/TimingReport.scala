package com.socrata.datacoordinator.util

import org.slf4j.Logger
import scala.util.DynamicVariable

trait TimingReport {
  def apply[T](name: String, kv: (String, Any)*)(f: => T): T
}

object NoopTimingReport extends TimingReport {
  def apply[T](name: String, kv: (String, Any)*)(f: => T): T = f
}

trait StackedTimingReport extends TimingReport {
  private val stackLocal = new ThreadLocal[List[String]] {
    override def initialValue = Nil
  }

  def stack = stackLocal.get

  abstract override def apply[T](name: String, kv: (String, Any)*)(f: => T): T = {
    stackLocal.set(name :: stack)
    try {
      super.apply(stack.reverse.mkString("/"), kv: _*)(f)
    } finally {
      stackLocal.set(stack.tail)
    }
  }
}

class LoggedTimingReport(log: Logger) extends TimingReport {
  def apply[T](name: String, kv: (String, Any)*)(f: => T): T = {
    val start = System.nanoTime()
    try {
      f
    } finally {
      val end = System.nanoTime()
      log.info("{}: {}ms; {}", name, ((end - start)/1000000).asInstanceOf[AnyRef], kv)
    }
  }
}
