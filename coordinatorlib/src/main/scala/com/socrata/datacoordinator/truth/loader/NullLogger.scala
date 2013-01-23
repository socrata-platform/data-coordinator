package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.truth.metadata.{VersionInfo, ColumnInfo}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class NullLogger[CV] extends Logger[CV] {
  def columnCreated(info: ColumnInfo) {}

  def columnRemoved(info: ColumnInfo) {}

  def rowIdentifierSet(info: ColumnInfo) {}

  def rowIdentifierCleared(info: ColumnInfo) {}

  def systemIdColumnSet(info: ColumnInfo) {}

  def workingCopyCreated(info: VersionInfo) {}

  def dataCopied() {}

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
