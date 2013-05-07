package com.socrata.datacoordinator
package truth.loader
package sql

import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.id.{RowId, ColumnId}
import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, RepBasedSqlDatasetContext}

class TestDatasetContext(val schema: ColumnIdMap[SqlColumnRep[TestColumnType, TestColumnValue]], val systemIdColumn: ColumnId, val userPrimaryKeyColumn: Option[ColumnId], val versionColumn: ColumnId) extends RepBasedSqlDatasetContext[TestColumnType, TestColumnValue] {
  val typeContext = TestTypeContext

  val systemColumnIds = ColumnIdSet(systemIdColumn)

  val userPrimaryKeyType = userPrimaryKeyColumn.map(schema(_).representedType)

  def mergeRows(a: Row[TestColumnValue], b: Row[TestColumnValue]) = a ++ b

  val primaryKeyColumn: ColumnId = userPrimaryKeyColumn.getOrElse(systemIdColumn)
  val primaryKeyType: TestColumnType = schema(primaryKeyColumn).representedType
}
