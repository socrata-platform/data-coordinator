package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.id.{RowVersion, RowId}

class RowDataProvider(initial: Long) {
  private var next = initial
  private var finished = false

  def allocate() = synchronized {
    if(finished) throw new IllegalStateException("Already finished")
    val result = next
    next += 1
    result
  }

  def finish(): Long = synchronized {
    finished = true
    next
  }

  def release() {}
}

class RowIdProvider(val underlying: RowDataProvider) extends AnyVal {
  def allocate() = new RowId(underlying.allocate())
}

class RowVersionProvider(val underlying: RowDataProvider) extends AnyVal {
  def allocate() = new RowVersion(underlying.allocate())
}

