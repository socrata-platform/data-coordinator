package com.socrata.datacoordinator
package truth.loader

import java.io.Closeable

import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.truth.metadata.ColumnInfo

trait Delogger[CV] extends Closeable {
  def delog(version: Long): CloseableIterator[Delogger.LogEvent[CV]]
}

object Delogger {
  sealed abstract class LogEvent[+CV]
  case class Truncated(schema: Map[ColumnId, ColumnInfo]) extends LogEvent[Nothing]
  case class ColumnCreated(info: ColumnInfo) extends LogEvent[Nothing]
  case class ColumnRemoved(info: ColumnInfo) extends LogEvent[Nothing]
  case class RowIdentifierChanged(info: Option[ColumnInfo]) extends LogEvent[Nothing]
  case object WorkingCopyCreated extends LogEvent[Nothing]
  case object WorkingCopyDropped extends LogEvent[Nothing]
  case object WorkingCopyPublished extends LogEvent[Nothing]
  case class RowDataUpdated[CV](operations: Seq[Operation[CV]]) extends LogEvent[CV]
  case object EndTransaction extends LogEvent[Nothing]

  sealed abstract class Operation[+CV]
  case class Insert[CV](systemId: Long, data: Row[CV]) extends Operation[CV]
  case class Update[CV](systemId: Long, data: Row[CV]) extends Operation[CV]
  case class Delete(systemId: Long) extends Operation[Nothing]
}
