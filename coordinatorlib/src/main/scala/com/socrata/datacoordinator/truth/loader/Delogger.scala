package com.socrata.datacoordinator
package truth.loader

import java.io.Closeable

import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.RowId

trait Delogger[CV] extends Closeable {
  def delog(version: Long): CloseableIterator[Delogger.LogEvent[CV]]
}

object Delogger {
  sealed abstract class LogEvent[+CV]
  case class Truncated(schema: ColumnIdMap[ColumnInfo]) extends LogEvent[Nothing]
  case class ColumnCreated(info: ColumnInfo) extends LogEvent[Nothing]
  case class ColumnRemoved(info: ColumnInfo) extends LogEvent[Nothing]
  case class RowIdentifierSet(info: ColumnInfo) extends LogEvent[Nothing]
  case class RowIdentifierCleared(info: ColumnInfo) extends LogEvent[Nothing]
  case class SystemRowIdentifierChanged(info: ColumnInfo) extends LogEvent[Nothing]
  case class WorkingCopyCreated(copyInfo: CopyInfo) extends LogEvent[Nothing]
  case object WorkingCopyDropped extends LogEvent[Nothing]
  case object WorkingCopyPublished extends LogEvent[Nothing]
  case class RowDataUpdated[CV](operations: Seq[Operation[CV]]) extends LogEvent[CV]
  case class RowIdCounterUpdated(nextRowId: RowId) extends LogEvent[Nothing]
  case object EndTransaction extends LogEvent[Nothing]
}
