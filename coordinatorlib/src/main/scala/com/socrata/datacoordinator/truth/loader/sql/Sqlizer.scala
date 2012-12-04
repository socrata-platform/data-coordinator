package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.{Connection, PreparedStatement, ResultSet}
import util.CloseableIterator

trait Sqlizer {
  def logTransactionComplete() // whole-database log has : (dataset id, last updated at, new txn log serial id)
  def lockTableAgainstWrites(table: String): String
}

/** Generates SQL for execution. */
trait DataSqlizer[CT, CV] extends Sqlizer {
  def datasetContext: DatasetContext[CT, CV]
  def typeContext: TypeContext[CV]

  def dataTableName: String
  def logTableName: String

  def softMaxBatchSize: Int
  def sizeofDelete: Int
  def sizeofInsert(row: Row[CV]): Int
  def sizeofUpdate(row: Row[CV]): Int

  def insertBatch(conn: Connection)(t: Inserter => Unit): Long
  trait Inserter {
    def insert(systemID: Long, row: Row[CV])
  }

  def prepareSystemIdDeleteStatement: String

  def prepareSystemIdDelete(stmt: PreparedStatement, sid: Long)
  def sqlizeSystemIdUpdate(sid: Long, row: Row[CV]): String

  def prepareUserIdDeleteStatement: String

  def prepareUserIdDelete(stmt: PreparedStatement, id: CV)
  def sqlizeUserIdUpdate(row: Row[CV]): String

  // txn log has (serial, row id, who did the update)
  type LogAuxColumn // The type of the JVM-side representation of the log's "aux data" column
  def findCurrentVersion: String
  def newRowAuxDataAccumulator(rowWriter: (LogAuxColumn) => Unit): RowAuxDataAccumulator
  def prepareLogRowsChangedStatement: String
  def prepareLogRowsChanged(stmt: PreparedStatement, version: Long, subversion: Long, rowsJson: LogAuxColumn): Int

  trait RowAuxDataAccumulator {
    def insert(systemID: Long, row: Row[CV])
    def update(sid: Long, row: Row[CV])
    def delete(systemID: Long)
    def finish()
  }

  // THIS MUST ONLY BE CALLED IF THIS DATASET HAS A USER PK COLUMN!
  def findSystemIds(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[IdPair[CV]]]
}

case class IdPair[+CV](systemId: Long, userId: CV)

trait SchemaSqlizer[CT, CV] extends Sqlizer {
  // all these include log-generation statements in their output
  def addColumn(column: String, typ: CT): Iterator[String]
  def dropColumn(column: String, typ: CT): Iterator[String]
  def renameColumn(oldName: String, newName: String): Iterator[String]
  def setPrimaryKeyColumn(column: String): Iterator[String]
  def copyTable(): Iterator[String] // this is also responsible for _creating_ the target table
}


