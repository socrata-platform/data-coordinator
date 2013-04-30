package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.id.{RowVersion, RowId}

class RowDataProvider(initial: Long) {
  private var next = initial
  private var finished = false

  private def allocate() = synchronized {
    if(finished) throw new IllegalStateException("Already finished")
    val result = next
    next += 1
    result
  }

  def allocateId() = new RowId(allocate())

  def allocateVersion() = new RowVersion(allocate())

  def finish(): Long = synchronized {
    finished = true
    next
  }

  def release() {}
}
