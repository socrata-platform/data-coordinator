package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.id.UserColumnId

trait SchemaLoader[CT] {
  def create(copyInfo: CopyInfo)
  def drop(copyInfo: CopyInfo)

  def addColumns(colInfo: Iterable[ColumnInfo[CT]])
  def dropColumns(colInfo: Iterable[ColumnInfo[CT]])

  def dropComputationStrategy(colInfo: ColumnInfo[CT])
  def updateFieldName(colInfo: ColumnInfo[CT])

  def makePrimaryKey(colInfo: ColumnInfo[CT])
  def makeSystemPrimaryKey(colInfo: ColumnInfo[CT])
  def makeVersion(colInfo: ColumnInfo[CT])
  def dropPrimaryKey(colInfo: ColumnInfo[CT]): Boolean

  sealed abstract class PrimaryKeyCreationException extends Exception
  case class NotPKableType(column: UserColumnId, typ: CT) extends PrimaryKeyCreationException
  case class NullValuesInColumn(column: UserColumnId) extends PrimaryKeyCreationException
  case class DuplicateValuesInColumn(column: UserColumnId) extends PrimaryKeyCreationException
}
