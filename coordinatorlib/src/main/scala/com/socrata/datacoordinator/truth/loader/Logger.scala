package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.util.collection.ColumnIdMap

trait Logger[CV] extends DataLogger[CV] {
  def truncated()
  def columnCreated(info: ColumnInfo)
  def columnRemoved(info: ColumnInfo)
  def rowIdentifierSet(newIdentifier: ColumnInfo)
  def rowIdentifierCleared(oldIdentifier: ColumnInfo)
  def systemIdColumnSet(info: ColumnInfo)
  def workingCopyCreated(info: CopyInfo)
  def logicalNameChanged(info: ColumnInfo)
  def dataCopied()
  def copyDropped(info: CopyInfo)
  def workingCopyPublished()

  /** Logs the end of the transaction and returns its version number.
   * @return The new log version number, or None if no other method was called. */
  def endTransaction(): Option[Long]
}
