package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.{Connection, PreparedStatement}

import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.truth.{DatasetContext, TypeContext}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.util.collection.RowIdSet

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

  def deleteBatch(conn: Connection)(f: Deleter => Unit): Long
  trait Deleter {
    def delete(sid: RowId)
  }

  def prepareSystemIdUpdateStatement: String
  def prepareSystemIdUpdate(stmt: PreparedStatement, sid: RowId, row: Row[CV])

  def findRows(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[RowWithId[CV]]]

  // THIS MUST ONLY BE CALLED IF THIS DATASET HAS A USER PK COLUMN!
  def findSystemIds(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[IdPair[CV]]]

  def collectSystemIds(conn: Connection, ids: Iterator[RowId]): CloseableIterator[RowIdSet]
}

case class RowWithId[CV](rowId: RowId, row: Row[CV])
case class IdPair[CV](systemId: RowId, userId: CV)

trait SchemaSqlizer[CT, CV] {
  // all these include log-generation statements in their output
  def addColumn(column: String, typ: CT): Iterator[String]
  def dropColumn(column: String, typ: CT): Iterator[String]
  def renameColumn(oldName: String, newName: String): Iterator[String]
  def setPrimaryKeyColumn(column: String): Iterator[String]
  def copyTable(): Iterator[String] // this is also responsible for _creating_ the target table
}


