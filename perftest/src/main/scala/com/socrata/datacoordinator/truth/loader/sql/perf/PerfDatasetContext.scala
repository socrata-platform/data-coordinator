package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import scala.collection.JavaConverters._

import com.socrata.datacoordinator.truth.{RowUserIdMap, DatasetContext}
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlColumnRep}

class PerfDatasetContext(val schema: ColumnIdMap[SqlColumnRep[PerfType, PerfValue]], val systemIdColumn: ColumnId, val userPrimaryKeyColumn: Option[ColumnId]) extends RepBasedSqlDatasetContext[PerfType, PerfValue] {
  val typeContext = PerfTypeContext

  userPrimaryKeyColumn.foreach { pkCol =>
    require(schema.contains(pkCol), "PK col defined but does not exist in the schema")
  }

  val userPrimaryKeyType = userPrimaryKeyColumn.map(schema(_).representedType)

  def userPrimaryKey(row: Row[PerfValue]) = for {
    userPKColumn <- userPrimaryKeyColumn
    value <- row.get(userPKColumn)
  } yield value

  def systemId(row: Row[PerfValue]) =
    row.get(systemIdColumn).map(_.asInstanceOf[PVId].value)

  def systemIdAsValue(row: Row[PerfValue]) = row.get(systemIdColumn)

  val systemColumnIds = ColumnIdSet(systemIdColumn)

  def mergeRows(a: Row[PerfValue], b: Row[PerfValue]) = a ++ b
}
