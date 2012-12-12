package com.socrata.datacoordinator
package truth

import com.socrata.datacoordinator.util.collection.LongLikeMap

/** Extracts information about a dataset and from rows within the context of a dataset. */
trait DatasetContext[CT, CV] {
  def hasCopy: Boolean

  def userSchema: LongLikeMap[ColumnId, CT]

  def userPrimaryKeyColumn: Option[ColumnId]
  def hasUserPrimaryKey: Boolean = userPrimaryKeyColumn.isDefined
  def userPrimaryKey(row: Row[CV]): Option[CV]

  def systemId(row: Row[CV]): Option[RowId]
  def systemIdAsValue(row: Row[CV]): Option[CV]

  def systemColumns(row: Row[CV]): Set[ColumnId]
  def systemSchema: LongLikeMap[ColumnId, CT]
  def systemIdColumnName: ColumnId

  def fullSchema: LongLikeMap[ColumnId, CT]

  def makeIdMap[T](): RowIdMap[CV, T]

  def primaryKeyColumn: ColumnId = userPrimaryKeyColumn.getOrElse(systemIdColumnName)

  def mergeRows(base: Row[CV], overlay: Row[CV]): Row[CV]
}
