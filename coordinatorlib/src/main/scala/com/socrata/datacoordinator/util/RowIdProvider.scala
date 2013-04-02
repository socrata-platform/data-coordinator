package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.id.{RowIdProcessor, RowId}

class RowIdProvider(initial: RowId, processor: RowIdProcessor) {
  private var next = initial.numeric
  private var finished = false

  def allocate() = synchronized {
    if(finished) throw new IllegalStateException("Already finished")
    val result = next
    next += 1
    processor(result)
  }

  def finish(): RowId = synchronized {
    finished = true
    processor(next)
  }

  def release() {}
}
