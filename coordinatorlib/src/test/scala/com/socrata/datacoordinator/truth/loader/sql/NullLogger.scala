package com.socrata.datacoordinator
package truth.loader.sql

import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.truth.metadata.ColumnInfo

class NullLogger[CV] extends Logger[CV] {
  def columnCreated(info: ColumnInfo) {}

  def columnRemoved(info :ColumnInfo) {}

  def rowIdentifierChanged(name: Option[ColumnInfo]) {}

  def workingCopyCreated() {}

  def workingCopyDropped() {}

  def workingCopyPublished() {}

  def endTransaction() = None

  def insert(systemID: Long, row: Row[CV]) {}

  def update(sid: Long, row: Row[CV]) {}

  def delete(systemID: Long) {}

  def close() {}

  def truncated(schema: Map[ColumnId, ColumnInfo]) {}
}

object NullLogger extends NullLogger[Any] {
  def apply[A]() = this.asInstanceOf[NullLogger[A]]
}
