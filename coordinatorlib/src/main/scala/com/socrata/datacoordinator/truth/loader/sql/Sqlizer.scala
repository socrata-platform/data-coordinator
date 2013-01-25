package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.{Connection, PreparedStatement}

import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.truth.{DatasetContext, TypeContext}
import com.socrata.datacoordinator.id.RowId

/** Generates SQL for execution. */
trait DataSqlizer[CT, CV] {
  def datasetContext: DatasetContext[CT, CV]
  def typeContext: TypeContext[CT, CV]

  def dataTableName: String

  def softMaxBatchSize: Int
  def sizeofDelete: Int
  def sizeofInsert(row: Row[CV]): Int
  def sizeofUpdate(row: Row[CV]): Int

  def insertBatch(conn: Connection)(t: Inserter => Unit): Long
  trait Inserter {
    def insert(row: Row[CV])
  }

  // TODO: Remove all this delete code in favor of batched system ID deletes
  def prepareSystemIdDeleteStatement: String

  def prepareSystemIdDelete(stmt: PreparedStatement, sid: RowId)
  def sqlizeSystemIdUpdate(sid: RowId, row: Row[CV]): String

  def sqlizeUserIdUpdate(row: Row[CV]): String

  // THIS MUST ONLY BE CALLED IF THIS DATASET HAS A USER PK COLUMN!
  def findSystemIds(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[IdPair[CV]]]
}

case class IdPair[+CV](systemId: RowId, userId: CV)

trait SchemaSqlizer[CT, CV] {
  // all these include log-generation statements in their output
  def addColumn(column: String, typ: CT): Iterator[String]
  def dropColumn(column: String, typ: CT): Iterator[String]
  def renameColumn(oldName: String, newName: String): Iterator[String]
  def setPrimaryKeyColumn(column: String): Iterator[String]
  def copyTable(): Iterator[String] // this is also responsible for _creating_ the target table
}


