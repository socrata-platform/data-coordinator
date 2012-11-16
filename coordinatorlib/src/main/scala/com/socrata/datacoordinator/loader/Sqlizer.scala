package com.socrata.datacoordinator.loader

import java.sql.{PreparedStatement, ResultSet}

trait Sqlizer {
  def logTransactionComplete() // whole-database log has : (dataset id, last updated at, new txn log serial id)
  def lockTableAgainstWrites(table: String): String
}

/** Generates SQL for execution. */
trait DataSqlizer[CT, CV] extends Sqlizer {
  def dataTableName: String
  def datasetContext: DatasetContext[CT, CV]

  def softMaxBatchSize: Int
  def sizeofDelete: Int
  def sizeofInsert(row: Row[CV]): Int
  def sizeofUpdate(row: Row[CV]): Int

  def prepareSystemIdDeleteStatement: String
  def prepareSystemIdInsertStatement: String

  def prepareSystemIdDelete(stmt: PreparedStatement, sid: Long)
  def prepareSystemIdInsert(stmt: PreparedStatement, sid: Long, row: Row[CV])
  def sqlizeSystemIdUpdate(sid: Long, row: Row[CV]): String

  def prepareUserIdDeleteStatement: String
  def prepareUserIdInsertStatement: String

  def prepareUserIdDelete(stmt: PreparedStatement, id: CV)
  def prepareUserIdInsert(stmt: PreparedStatement, sid: Long, row: Row[CV])
  def sqlizeUserIdUpdate(row: Row[CV]): String

  // txn log has (serial, row id, who did the update)
  type LogAuxColumn // The type of the JVM-side representation of the log's "aux data" column
  def findCurrentVersion: String
  def newRowAuxDataAccumulator(rowWriter: (LogAuxColumn) => Unit): RowAuxDataAccumulator
  def prepareLogRowsChangedStatement: String
  def prepareLogRowsChanged(stmt: PreparedStatement, version: Long, rowsJson: LogAuxColumn): Int

  trait RowAuxDataAccumulator {
    def insert(systemID: Long, row: Row[CV])
    def update(sid: Long, row: Row[CV])
    def delete(systemID: Long)
    def finish()
  }

  def selectRow(id: CV): String

  def extract(resultSet: ResultSet, logicalColumn: String): CV

  def extractRow(resultSet: ResultSet): Row[CV] =
    datasetContext.fullSchema.keys.foldLeft(Map.empty[String, CV]) { (results, col) =>
      results + (col -> extract(resultSet, col))
    }

  // This may batch the "ids" into multiple queries.  The queries
  // returned will contain two columns: "sid" and "uid".  THIS MUST
  // ONLY BE CALLED IF THIS DATASET HAS A USER PK COLUMN!
  def findSystemIds(ids: Iterator[CV]): Iterator[String]
  def extractIdPairs(rs: ResultSet): Iterator[IdPair[CV]]
}

case class IdPair[+CV](systemId: Long, userId: CV)

trait SchemaSqlizer[CT, CV] extends Sqlizer {
  // all these include log-generation statements in their output
  def addColumn(column: String, typ: CT): Iterator[String]
  def dropColumn(column: String, typ: CT): Iterator[String]
  def setPrimaryKeyColumn(column: String): Iterator[String]
  def copyTable(targetTable: String): Iterator[String] // this is also responsible for _creating_ the target table
}


