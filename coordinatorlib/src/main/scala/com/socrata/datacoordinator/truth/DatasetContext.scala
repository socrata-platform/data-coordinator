package com.socrata.datacoordinator
package truth

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.{RowId, ColumnId}

/** Extracts information about a dataset and from rows within the context of a dataset. */
trait DatasetContext[CT, CV] {
  def typeContext: TypeContext[CT, CV]

  def userSchema: ColumnIdMap[CT]

  def userPrimaryKeyColumn: Option[ColumnId]
  def hasUserPrimaryKey: Boolean = userPrimaryKeyColumn.isDefined
  def userPrimaryKey(row: Row[CV]): Option[CV]

  def systemId(row: Row[CV]): Option[RowId]
  def systemIdAsValue(row: Row[CV]): Option[CV]

  def systemColumns(row: Row[CV]): Set[ColumnId]
  def systemSchema: ColumnIdMap[CT]
  def systemIdColumn: ColumnId

  def fullSchema: ColumnIdMap[CT]

  def makeIdMap[T](): RowIdMap[CV, T]

  def primaryKeyColumn: ColumnId = userPrimaryKeyColumn.getOrElse(systemIdColumn)

  def mergeRows(base: Row[CV], overlay: Row[CV]): Row[CV]
}
