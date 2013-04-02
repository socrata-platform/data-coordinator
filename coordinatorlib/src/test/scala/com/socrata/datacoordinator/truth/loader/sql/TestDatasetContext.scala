package com.socrata.datacoordinator
package truth.loader
package sql

import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.id.{RowIdProcessor, RowId, ColumnId}
import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, RepBasedSqlDatasetContext}

class TestDatasetContext(val schema: ColumnIdMap[SqlColumnRep[TestColumnType, TestColumnValue]], val systemIdColumn: ColumnId, val userPrimaryKeyColumn: Option[ColumnId], ridProc: RowIdProcessor) extends RepBasedSqlDatasetContext[TestColumnType, TestColumnValue] {
  val typeContext = new TestTypeContext(ridProc)

  val systemColumnIds = ColumnIdSet(systemIdColumn)

  userPrimaryKeyColumn.foreach { pkCol =>
    require(userColumnIds.contains(pkCol), "PK col defined but does not exist in the schema")
  }

  val userPrimaryKeyType = userPrimaryKeyColumn.map(schema(_).representedType)

  def userPrimaryKey(row: Row[TestColumnValue]) = for {
    userPKColumn <- userPrimaryKeyColumn
    value <- row.get(userPKColumn)
  } yield value

  def systemId(row: Row[TestColumnValue]) =
    row.get(systemIdColumn).map { i => ridProc(i.asInstanceOf[LongValue].value) }

  def systemIdAsValue(row: Row[TestColumnValue]) = row.get(systemIdColumn)

  def mergeRows(a: Row[TestColumnValue], b: Row[TestColumnValue]) = a ++ b
}
