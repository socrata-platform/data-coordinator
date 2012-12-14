package com.socrata.datacoordinator
package truth.loader

import java.io.Closeable
import util.CloseableIterator

trait Delogger[CT, CV] extends Closeable {
  def delog(version: Long): CloseableIterator[Delogger.LogEvent[CT, CV]]
}

object Delogger {
  sealed abstract class LogEvent[+CT, +CV]
  case class Truncated[CT](schema: Map[ColumnId, CT]) extends LogEvent[CT, Nothing]
  case class ColumnCreated[CT](name: String, typ: CT) extends LogEvent[CT, Nothing]
  case class ColumnRemoved(name: String) extends LogEvent[Nothing, Nothing]
  case class RowIdentifierChanged(name: Option[String]) extends LogEvent[Nothing, Nothing]
  case object WorkingCopyCreated extends LogEvent[Nothing, Nothing]
  case object WorkingCopyDropped extends LogEvent[Nothing, Nothing]
  case object WorkingCopyPublished extends LogEvent[Nothing, Nothing]
  case class RowDataUpdated[CV](operations: Seq[Operation[CV]]) extends LogEvent[Nothing, CV]
  case object EndTransaction extends LogEvent[Nothing, Nothing]

  sealed abstract class Operation[+CV]
  case class Insert[CV](systemId: Long, data: Row[CV]) extends Operation[CV]
  case class Update[CV](systemId: Long, data: Row[CV]) extends Operation[CV]
  case class Delete(systemId: Long) extends Operation[Nothing]
}
