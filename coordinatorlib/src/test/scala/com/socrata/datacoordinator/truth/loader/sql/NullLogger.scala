package com.socrata.datacoordinator
package truth.loader.sql

import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.RowId

class NullLogger[CV] extends Logger[CV] {
  def columnCreated(info: ColumnInfo) {}

  def columnRemoved(info :ColumnInfo) {}

  def rowIdentifierChanged(name: Option[ColumnInfo]) {}

  def workingCopyCreated() {}

  def workingCopyDropped() {}

  def workingCopyPublished() {}

  def endTransaction() = None

  def insert(systemID: RowId, row: Row[CV]) {}

  def update(sid: RowId, row: Row[CV]) {}

  def delete(systemID: RowId) {}

  def close() {}

  def truncated(schema: ColumnIdMap[ColumnInfo]) {}
}

object NullLogger extends NullLogger[Any] {
  def apply[A]() = this.asInstanceOf[NullLogger[A]]
}
