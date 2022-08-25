package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.{Connection, PreparedStatement}

import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.truth.{DatasetContext, TypeContext}
import com.socrata.datacoordinator.id.{ColumnId, RowVersion, RowId}

trait ReadDataSqlizer[CT, CV] {
  def datasetContext: DatasetContext[CT, CV]
  def typeContext: TypeContext[CT, CV]

  def dataTableName: String

  def findRows(conn: Connection, bySystemId: Boolean, ids: Iterator[CV], explain: Boolean = false): CloseableIterator[Seq[InspectedRow[CV]]]
  // When finding rows, it will be performed (and returned) in chunks
  // of this size (note that if you ask for rows that don't exist or
  // provide duplicate IDs, the produced chunks may be smaller than
  // requested)
  val findRowsBlockSize: Int
}

/** Generates SQL for execution. */
trait DataSqlizer[CT, CV] extends ReadDataSqlizer[CT, CV] {
  type PreloadStatistics

  def computeStatistics(conn: Connection): PreloadStatistics
  def updateStatistics(conn: Connection, rowsAdded: Long, rowsDeleted: Long, rowsChanged: Long, preload: PreloadStatistics): PreloadStatistics

  def softMaxBatchSize: Int
  def sizeofDelete(id: CV, bySystemIdForced: Boolean): Int
  def sizeof(row: Row[CV]): Int

  def insertBatch[T](conn: Connection)(t: Inserter => T): (Long, T)
  trait Inserter {
    def insert(row: Row[CV])
  }

  def deleteBatch[T](conn: Connection, explain: Boolean = false)(f: Deleter => T): (Long, T)
  trait Deleter {
    def delete(sid: RowId)
  }

  def doExplain(conn: Connection, sql: String, sqlFiller: PreparedStatement => Unit, idempotent: Boolean): Unit

  def prepareSystemIdUpdateStatement: String
  def prepareSystemIdUpdate(stmt: PreparedStatement, sid: RowId, row: Row[CV])
}

case class InspectedRow[CV](id: CV, rowId: RowId, version: RowVersion, row: Row[CV])
case class InspectedRowless[CV](id: CV, rowId: RowId, version: RowVersion)

trait SchemaSqlizer[CT, CV] {
  // all these include log-generation statements in their output
  def addColumn(column: String, typ: CT): Iterator[String]
  def dropColumn(column: String, typ: CT): Iterator[String]
  def renameColumn(oldName: String, newName: String): Iterator[String]
  def setPrimaryKeyColumn(column: String): Iterator[String]
  def copyTable(): Iterator[String] // this is also responsible for _creating_ the target table
}


