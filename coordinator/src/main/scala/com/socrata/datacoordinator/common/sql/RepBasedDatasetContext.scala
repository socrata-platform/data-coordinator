package com.socrata.datacoordinator
package common.sql

import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, RepBasedSqlDatasetContext}
import com.socrata.datacoordinator.truth.TypeContext
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.id.{RowId, ColumnId}

class RepBasedDatasetContext[CT, CV](val typeContext: TypeContext[CT, CV],
                                     val schema: ColumnIdMap[SqlColumnRep[CT, CV]],
                                     val userPrimaryKeyColumn: Option[ColumnId],
                                     val systemIdColumn: ColumnId,
                                     val systemColumnIds: ColumnIdSet)
  extends RepBasedSqlDatasetContext[CT, CV]
{
  val userPrimaryKeyType: Option[CT] = userPrimaryKeyColumn.map(schema(_).representedType)

  def userPrimaryKey(row: Row[CV]): Option[CV] =
    row.get(userPrimaryKeyColumn.get)

  def systemId(row: Row[CV]): Option[RowId] =
    row.get(systemIdColumn).map(typeContext.makeSystemIdFromValue)

  def systemIdAsValue(row: Row[CV]): Option[CV] =
    row.get(systemIdColumn)

  def mergeRows(base: Row[CV], overlay: Row[CV]): Row[CV] = base ++ overlay
}
