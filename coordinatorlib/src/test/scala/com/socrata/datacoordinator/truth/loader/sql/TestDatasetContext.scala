package com.socrata.datacoordinator
package truth.loader
package sql

import scala.collection.JavaConverters._

import com.socrata.datacoordinator.truth.{RowUserIdMap, DatasetContext}
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.id.{RowId, ColumnId}
import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, RepBasedSqlDatasetContext}

class TestDatasetContext(val schema: ColumnIdMap[SqlColumnRep[TestColumnType, TestColumnValue]], val systemIdColumn: ColumnId, val userPrimaryKeyColumn: Option[ColumnId]) extends RepBasedSqlDatasetContext[TestColumnType, TestColumnValue] {
  val typeContext = TestTypeContext

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
    row.get(systemIdColumn).map { i => new RowId(i.asInstanceOf[LongValue].value) }

  def systemIdAsValue(row: Row[TestColumnValue]) = row.get(systemIdColumn)

  def mergeRows(a: Row[TestColumnValue], b: Row[TestColumnValue]) = a ++ b
}
