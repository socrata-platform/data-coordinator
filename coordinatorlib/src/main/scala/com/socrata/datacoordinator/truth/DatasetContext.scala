package com.socrata.datacoordinator
package truth

import com.socrata.datacoordinator.util.collection.ColumnIdSet
import com.socrata.datacoordinator.id.{RowId, ColumnId}

/** Extracts information about a dataset and from rows within the context of a dataset. */
trait DatasetContext[CT, CV] {
  val typeContext: TypeContext[CT, CV]
  val userPrimaryKeyColumn: Option[ColumnId]
  val userPrimaryKeyType: Option[CT]
  val primaryKeyType: CT
  val systemIdColumn: ColumnId
  val allColumnIds: ColumnIdSet
  val userColumnIds: ColumnIdSet
  val systemColumnIds: ColumnIdSet

  lazy val hasUserPrimaryKey: Boolean = userPrimaryKeyColumn.isDefined

  def systemColumns(row: Row[CV]): ColumnIdSet = row.keySet.intersect(systemColumnIds)

  def makeIdMap[T](): RowUserIdMap[CV, T] = typeContext.makeIdMap(primaryKeyType)

  def mergeRows(base: Row[CV], overlay: Row[CV]): Row[CV]

  val primaryKeyColumn: ColumnId
  val versionColumn: ColumnId
}
