package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.id.{RowVersion, RowId}
import java.util.concurrent.atomic.AtomicLong

class RowDataProvider(initial: Long) {
  private val next = new AtomicLong(initial)

  def allocate(): Long = {
    while(true) {
      val result = next.get
      if(result == -1) throw new IllegalStateException("Already finished")
      if(next.compareAndSet(result, result + 1)) return result
    }
    sys.error("Can't get here")
  }

  def finish(): Long =
    next.getAndSet(-1)

  def release() {}
}

class RowIdProvider(val underlying: RowDataProvider) extends AnyVal {
  def allocate() = new RowId(underlying.allocate())
}

class RowVersionProvider(val underlying: RowDataProvider) extends AnyVal {
  def allocate() = new RowVersion(underlying.allocate())
}

