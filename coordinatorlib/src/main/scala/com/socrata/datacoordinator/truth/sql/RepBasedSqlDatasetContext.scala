package com.socrata.datacoordinator
package truth
package sql

import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.id.{RowId, ColumnId}

trait RepBasedSqlDatasetContext[CT, CV] extends DatasetContext[CT, CV] {
  val schema: ColumnIdMap[SqlColumnRep[CT, CV]]
  lazy val allColumnIds = schema.keySet
  lazy val userColumnIds = allColumnIds.filterNot(systemColumnIds)

  lazy val canonicallyOrderedColumnIds = allColumnIds.toSet.toSeq.sorted
  lazy val canonicallyOrderedUserColumnIds = userColumnIds.toSet.toSeq.sorted
}

object RepBasedSqlDatasetContext {
  private class Impl[CT, CV](val typeContext: TypeContext[CT, CV],
                             val schema: ColumnIdMap[SqlColumnRep[CT, CV]],
                             val userPrimaryKeyColumn: Option[ColumnId],
                             val systemIdColumn: ColumnId,
                             val versionColumn: ColumnId,
                             val systemColumnIds: ColumnIdSet)
    extends RepBasedSqlDatasetContext[CT, CV]
  {
    val userPrimaryKeyType: Option[CT] = userPrimaryKeyColumn.map(schema(_).representedType)
    val primaryKeyColumn = userPrimaryKeyColumn.getOrElse(systemIdColumn)
    val primaryKeyType = schema(primaryKeyColumn).representedType

    def mergeRows(base: Row[CV], overlay: Row[CV]): Row[CV] = base ++ overlay
  }

  def apply[CT, CV](typeContext: TypeContext[CT, CV],
                    schema: ColumnIdMap[SqlColumnRep[CT, CV]],
                    userPrimaryKeyColumn: Option[ColumnId],
                    systemIdColumn: ColumnId,
                    versionColumn: ColumnId,
                    systemColumnIds: ColumnIdSet): RepBasedSqlDatasetContext[CT, CV] =
    new Impl(typeContext, schema, userPrimaryKeyColumn, systemIdColumn, versionColumn, systemColumnIds)
}
