package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.soql.environment.ColumnName

trait SchemaLoader[CT] {
  def create(copyInfo: CopyInfo)
  def drop(copyInfo: CopyInfo)

  def addColumn(colInfo: ColumnInfo[CT])
  def dropColumn(colInfo: ColumnInfo[CT])

  def makePrimaryKey(colInfo: ColumnInfo[CT])
  def makeSystemPrimaryKey(colInfo: ColumnInfo[CT])
  def dropPrimaryKey(colInfo: ColumnInfo[CT]): Boolean

  sealed abstract class PrimaryKeyCreationException extends Exception
  case class NotPKableType(column: ColumnName, typ: CT) extends PrimaryKeyCreationException
  case class NullValuesInColumn(column: ColumnName) extends PrimaryKeyCreationException
  case class DuplicateValuesInColumn(column: ColumnName) extends PrimaryKeyCreationException
}
