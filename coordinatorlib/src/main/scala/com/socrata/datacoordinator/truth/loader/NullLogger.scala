package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class NullLogger[CV] extends Logger[CV] {
  def columnCreated(info: ColumnInfo) {}

  def columnRemoved(info: ColumnInfo) {}

  def rowIdentifierChanged(name: Option[ColumnInfo]) {}

  def systemIdColumnSet(info: ColumnInfo) {}

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
  def apply[A] = this.asInstanceOf[NullLogger[A]]
}
