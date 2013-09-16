package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.{Connection, PreparedStatement}

import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.truth.{DatasetContext, TypeContext}
import com.socrata.datacoordinator.id.{RowVersion, RowId}

trait ReadDataSqlizer[CT, CV] {
  def datasetContext: DatasetContext[CT, CV]
  def typeContext: TypeContext[CT, CV]

  def dataTableName: String

  // convenience method; like calling findRowsSubset with all column IDs in the
  // dataset.
  def findRows(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[InspectedRow[CV]]]
  // Not sure this will survive the row version feature
  def findIdsAndVersions(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[InspectedRowless[CV]]]
}

/** Generates SQL for execution. */
trait DataSqlizer[CT, CV] extends ReadDataSqlizer[CT, CV] {
  type PreloadStatistics

  def computeStatistics(conn: Connection): PreloadStatistics
  def updateStatistics(conn: Connection, rowsAdded: Long, rowsDeleted: Long, rowsChanged: Long, preload: PreloadStatistics)

  def softMaxBatchSize: Int
  def sizeofDelete(id: CV): Int
  def sizeof(row: Row[CV]): Int

  def insertBatch[T](conn: Connection)(t: Inserter => T): (Long, T)
  trait Inserter {
    def insert(row: Row[CV])
  }

  def deleteBatch[T](conn: Connection)(f: Deleter => T): (Long, T)
  trait Deleter {
    def delete(sid: RowId)
  }

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


