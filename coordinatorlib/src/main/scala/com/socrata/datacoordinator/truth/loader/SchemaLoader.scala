package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.soql.environment.{TypeName, ColumnName}

trait SchemaLoader {
  def create(copyInfo: CopyInfo)
  def drop(copyInfo: CopyInfo)

  def addColumn(colInfo: ColumnInfo)
  def dropColumn(colInfo: ColumnInfo)

  def makePrimaryKey(colInfo: ColumnInfo)
  def makeSystemPrimaryKey(colInfo: ColumnInfo)
  def dropPrimaryKey(colInfo: ColumnInfo): Boolean

  sealed abstract class PrimaryKeyCreationException extends Exception
  case class NotPKableType(column: ColumnName, typ: TypeName) extends PrimaryKeyCreationException
  case class NullValuesInColumn(column: ColumnName) extends PrimaryKeyCreationException
  case class DuplicateValuesInColumn(column: ColumnName) extends PrimaryKeyCreationException
}
