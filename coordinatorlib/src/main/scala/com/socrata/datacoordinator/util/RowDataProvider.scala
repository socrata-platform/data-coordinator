package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.id.{RowVersion, RowId}
import java.util.concurrent.atomic.AtomicLong

class RowDataProvider(initial: Long) {
  private val next = new AtomicLong(initial)

  private def alreadyFinished() =
    throw new IllegalStateException("Already finished")

  def allocate(): Long = {
    while(true) {
      val result = next.get
      if(result == -1) alreadyFinished()
      if(next.compareAndSet(result, result + 1)) return result
    }
    sys.error("Can't get here")
  }

  def finish(): Long = {
    val result = next.getAndSet(-1)
    if(result == -1) alreadyFinished()
    result
  }

  def release() {}
}

class RowIdProvider(val underlying: RowDataProvider) extends AnyVal {
  def allocate() = new RowId(underlying.allocate())
}

class RowVersionProvider(val underlying: RowDataProvider) extends AnyVal {
  def allocate() = new RowVersion(underlying.allocate())
}

