package com.socrata.datacoordinator
package common.sql

import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, RepBasedSqlDatasetContext}
import com.socrata.datacoordinator.truth.TypeContext
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.id.{RowId, ColumnId}

object RepBasedDatasetContextProvider {
  def apply[CT, CV](_typeContext: TypeContext[CT, CV], _schema: ColumnIdMap[SqlColumnRep[CT, CV]], userPKCol: Option[ColumnId], idCol: ColumnId, systemIds: ColumnIdSet): RepBasedSqlDatasetContext[CT, CV] =
    new RepBasedSqlDatasetContext[CT, CV] {
      val typeContext: TypeContext[CT, CV] = _typeContext

      val schema = _schema

      val systemColumnIds = systemIds

      val systemIdColumn = idCol

      val userPrimaryKeyColumn: Option[ColumnId] = userPKCol
      val userPrimaryKeyType: Option[CT] = userPKCol.map(schema(_).representedType)

      def userPrimaryKey(row: Row[CV]): Option[CV] =
        row.get(userPrimaryKeyColumn.get)

      def systemId(row: Row[CV]): Option[RowId] =
        row.get(systemIdColumn).map(typeContext.makeSystemIdFromValue)

      def systemIdAsValue(row: Row[CV]): Option[CV] =
        row.get(systemIdColumn)

      def mergeRows(base: Row[CV], overlay: Row[CV]): Row[CV] = base ++ overlay
    }
}
