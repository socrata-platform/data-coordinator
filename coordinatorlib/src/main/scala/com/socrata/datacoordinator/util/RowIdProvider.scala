package com.socrata.datacoordinator.util

import com.socrata.id.numeric.IdProvider
import com.socrata.datacoordinator.id.RowId

class RowIdProvider(initial: RowId) extends IdProvider {
  private var next = initial.underlying
  private var finished = false

  def allocate() = synchronized {
    if(finished) throw new IllegalStateException("Already finished")
    val result = next
    next += 1
    result
  }

  def finish(): RowId = synchronized {
    finished = true
    new RowId(next)
  }

  def release() {}
}
