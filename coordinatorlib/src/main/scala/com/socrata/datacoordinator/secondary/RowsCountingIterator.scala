package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.truth.loader.{ Delogger => Delogger }

class RowsCountingIterator[T](underlying: Iterator[T]) extends Iterator[T] {

  private var _rowsChanged = 0L

  def hasNext: Boolean = underlying.hasNext

  def next(): T = {
    val item = underlying.next()
    item match {
      case Delogger.RowsChangedPreview(rowsInserted, rowsUpdated, rowsDeleted, truncated) =>
        _rowsChanged += (rowsInserted + rowsUpdated + rowsDeleted)
      case _ =>
    }
    item
  }

  def rowsChanged = _rowsChanged
}