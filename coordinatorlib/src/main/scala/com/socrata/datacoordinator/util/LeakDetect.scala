package com.socrata.datacoordinator.util

import java.io.Closeable

trait LeakDetect extends Closeable {
  @volatile private var closed = false

  private val allocatedAt =
    try { throw new Exception("Stack trace") }
    catch { case e: Exception => e }

  abstract override def close() {
    closed = true
    super.close()
  }

  override def finalize() {
    if(!closed) org.slf4j.LoggerFactory.getLogger(classOf[LeakDetect]).warn("Unclosed object of type " + getClass + " detected", allocatedAt)
    super.finalize()
  }
}
