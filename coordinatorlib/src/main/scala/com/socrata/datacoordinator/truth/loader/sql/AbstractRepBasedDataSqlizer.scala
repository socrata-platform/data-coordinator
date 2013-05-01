package com.socrata.datacoordinator
package truth.loader.sql

import java.sql.{ResultSet, Connection, PreparedStatement}

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlPKableColumnRep, SqlColumnRep}
import com.socrata.datacoordinator.util.{CloseableIterator, FastGroupedIterator, LeakDetect}
import com.socrata.datacoordinator.id.RowId

abstract class AbstractRepBasedDataSqlizer[CT, CV](val dataTableName: String,
                                                   val datasetContext: RepBasedSqlDatasetContext[CT, CV])
  extends DataSqlizer[CT, CV]
{
  val typeContext = datasetContext.typeContext

  val nullValue = typeContext.nullValue

  val logicalPKColumnName = datasetContext.primaryKeyColumn

  val repSchema = datasetContext.schema

  val sidRep = repSchema(datasetContext.systemIdColumn).asInstanceOf[SqlPKableColumnRep[CT, CV]]
  val pkRep = repSchema(logicalPKColumnName).asInstanceOf[SqlPKableColumnRep[CT, CV]]

  def softMaxBatchSize = 2000000

  def sizeofDelete = 8

  def sizeofInsert(row: Row[CV]) = {
    var total = 0
    val it = row.iterator
    while(it.hasNext) {
      it.advance()
      total += repSchema(it.key).estimateInsertSize(it.value)
    }
    total
  }

  def sizeofUpdate(row: Row[CV]) = {
    var total = 0
    val it = row.iterator
    while(it.hasNext) {
      it.advance()
      total += repSchema(it.key).estimateUpdateSize(it.value)
    }
    total
  }

  val prepareSystemIdDeleteStatement =
    "DELETE FROM " + dataTableName + " WHERE " + sidRep.templateForSingleLookup

  def prepareSystemIdDelete(stmt: PreparedStatement, sid: RowId) {
    sidRep.prepareSingleLookup(stmt, typeContext.makeValueFromSystemId(sid), 1)
  }

  def prepareSystemIdUpdateStatement: String =
    "UPDATE " + dataTableName + " SET " + repSchema.values.flatMap(_.physColumns).map(_ + " = ?").mkString(",") + " WHERE " + sidRep.templateForSingleLookup

  def prepareSystemIdUpdate(stmt: PreparedStatement, sid: RowId, row: Row[CV]) {
    var i = 1
    for((cid, rep) <- repSchema) {
      val v = row.getOrElseStrict(cid, typeContext.nullValue)
      i = rep.prepareUpdate(stmt, v, i)
    }
    sidRep.prepareUpdate(stmt, typeContext.makeValueFromSystemId(sid), i)
  }

  def blockQueryById[T](conn: Connection, ids: Iterator[CV], prefix: String)(decode: ResultSet => T): CloseableIterator[Seq[T]] = {
    class ResultIterator extends CloseableIterator[Seq[T]] {
      val blockSize = 100
      val grouped = new FastGroupedIterator(ids, blockSize)
      var _fullStmt: PreparedStatement = null

      def fullStmt = {
        if(_fullStmt == null) _fullStmt = conn.prepareStatement(prefix + pkRep.templateForMultiLookup(blockSize))
        _fullStmt
      }

      def close() {
        if(_fullStmt != null) _fullStmt.close()
      }

      def hasNext = grouped.hasNext

      def next(): Seq[T] = {
        val block = grouped.next()
        if(block.size != blockSize) {
          using(conn.prepareStatement(prefix + pkRep.templateForMultiLookup(block.size))) { stmt2 =>
            fillAndResult(stmt2, block)
          }
        } else {
          fillAndResult(fullStmt, block)
        }
      }

      def fillAndResult(stmt: PreparedStatement, block: Seq[CV]): Seq[T] = {
        var i = 1
        for(cv <- block) {
          i = pkRep.prepareMultiLookup(stmt, cv, i)
        }
        using(stmt.executeQuery()) { rs =>
          val result = new scala.collection.mutable.ArrayBuffer[T](block.length)
          while(rs.next()) {
            result += decode(rs)
          }
          result
        }
      }
    }
    new ResultIterator with LeakDetect
  }

  val findRowsPrefix = "SELECT " + repSchema.values.flatMap(_.physColumns).mkString(",") + " FROM " + dataTableName + " WHERE "

  def findRows(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[RowWithId[CV]]] =
    blockQueryById(conn, ids, findRowsPrefix) { rs =>
      val row = new MutableRow[CV]
      var i = 1
      for((cid, rep) <- repSchema) {
        row(cid) = rep.fromResultSet(rs, i)
        i += rep.physColumns.length
      }
      RowWithId(typeContext.makeSystemIdFromValue(row(datasetContext.systemIdColumn)), row.freeze())
    }

  val findSystemIdsPrefix = "SELECT " + sidRep.physColumns.mkString(",") + "," + pkRep.physColumns.mkString(",") + " FROM " + dataTableName + " WHERE "
  def findSystemIds(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[IdPair[CV]]] = {
    require(datasetContext.hasUserPrimaryKey, "findSystemIds called without a user primary key")
    blockQueryById(conn, ids, findSystemIdsPrefix) { rs =>
      val sid = typeContext.makeSystemIdFromValue(sidRep.fromResultSet(rs, 1))
      val uid = pkRep.fromResultSet(rs, 1 + sidRep.physColumns.length)
      IdPair(sid, uid)
    }
  }
}
