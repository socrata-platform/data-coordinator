package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}

trait Logger[CT, CV] extends DataLogger[CV] {
  def truncated()
  def columnCreated(info: ColumnInfo[CT])
  def columnRemoved(info: ColumnInfo[CT])
  def rowIdentifierSet(newIdentifier: ColumnInfo[CT])
  def rowIdentifierCleared(oldIdentifier: ColumnInfo[CT])
  def systemIdColumnSet(info: ColumnInfo[CT])
  def versionColumnSet(info: ColumnInfo[CT])
  def workingCopyCreated(info: CopyInfo)
  def logicalNameChanged(info: ColumnInfo[CT])
  def dataCopied()
  def workingCopyDropped()
  def snapshotDropped(info: CopyInfo)
  def workingCopyPublished()

  /** Logs the end of the transaction and returns its version number.
   * @return The new log version number, or None if no other method was called. */
  def endTransaction(): Option[Long]
}
