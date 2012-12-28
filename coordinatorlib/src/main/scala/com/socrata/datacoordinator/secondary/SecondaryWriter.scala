package com.socrata.datacoordinator
package secondary

import java.io.Closeable
import com.socrata.datacoordinator.id.{ColumnId, RowId}

trait SecondaryWriter[CT, CV] extends Closeable {
  // A store should be aware that commands can be re-played.  Each one of
  // these operations corresponds to a row in the dataset's WAL log table.
  // As a result, if a replay happens to occur, the store can rely on
  // exactly the same sequence of events being generated (and so it can keep
  // track of how much it should ignore, if it does not have atomic transactions
  // itself).

  def currentVersion: Long // 0 if new

  // throws WorkingCopyUnsupported if the store isn't interested in working copies
  def makeWorkingCopy()

  // throws WorkingCopyUnsupported if the store isn't interested in working copies
  def dropWorkingCopy()

  // throws WorkingCopyUnsupported if the store isn't interested in working copies
  def publishWorkingCopy()

  def dataChanged(rows: Iterator[RowDataOperation[CV]])
  def truncate() // this should also clear the schema; it will be followed by a series of addColumns and maybe a userRowIdentifierChanged.
  def addColumn(column: ColumnId, typ: CT)
  def removeColumn(column: ColumnId)
  def convert(column: ColumnId, newTyp: CT)
  def userRowIdentifierChanged(newIdentifierColumn: Option[ColumnId])

  // Tells the store that this version is finished and a new one is starting.
  // This is allowed but not required to commit the changes made beforehand.
  // Note that versions MAY be empty.
  def finish()

  /**
   * @throws VersionUnfinished if changes were made and finish() was not called.
   */
  def commit()

  // This should (but is not required to) roll back any changes that were made
  // since the last finish() or commit() call.
  def close()
}

sealed trait RowDataOperation[+CV]
case class Delete(sid: RowId) extends RowDataOperation[Nothing]
case class Update[CV](sid: RowId, data: Row[CV]) extends RowDataOperation[CV]
case class Insert[CV](sid: RowId, data: Row[CV]) extends RowDataOperation[CV]
