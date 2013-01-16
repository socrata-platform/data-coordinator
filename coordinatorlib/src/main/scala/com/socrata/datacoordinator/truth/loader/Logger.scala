package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap

trait Logger[CV] extends DataLogger[CV] {
  def truncated(schema: ColumnIdMap[ColumnInfo])
  def columnCreated(info: ColumnInfo)
  def columnRemoved(info: ColumnInfo)
  def rowIdentifierChanged(newIdentifier: Option[ColumnInfo])
  def systemIdColumnSet(info: ColumnInfo)
  def workingCopyCreated()
  def workingCopyDropped()
  def workingCopyPublished()

  /** Logs the end of the transaction and returns its version number.
   * @return The new log version number, or None if no other method was called. */
  def endTransaction(): Option[Long]
}
