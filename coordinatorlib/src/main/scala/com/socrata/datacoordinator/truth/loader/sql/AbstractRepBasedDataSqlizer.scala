package com.socrata.datacoordinator
package truth.loader.sql

import java.sql.{ResultSet, Connection, PreparedStatement}

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlPKableColumnRep, SqlColumnRep}
import com.socrata.datacoordinator.util.{CloseableIterator, FastGroupedIterator, LeakDetect}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.util.collection.{RowIdSet, MutableRowIdSet}
import java.io.Closeable

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

  def deleteBatch(conn: Connection)(f: Deleter => Unit): Long = {
    using(new DeleterImpl(conn)) { deleter =>
      f(deleter)
      deleter.finish()
    }
  }

  class DeleterImpl(conn: Connection) extends Deleter with Closeable {
    private val rowSet = MutableRowIdSet()
    private var count = 0L
    private val batchSize = 100
    private var _fullStmt: PreparedStatement = null

    private def deleteSql(n: Int) = "DELETE FROM " + dataTableName + " WHERE " + sidRep.templateForMultiLookup(n)

    private def fullStmt = {
      if(_fullStmt == null) _fullStmt = conn.prepareStatement(deleteSql(batchSize))
      _fullStmt
    }

    private def doFlush(stmt: PreparedStatement) {
      var i = 1
      for(sid <- rowSet) {
        stmt.setLong(i, sid.underlying)
        i += 1
      }
      count += stmt.executeUpdate()
    }

    private def flush() {
      if(rowSet.size == batchSize) doFlush(fullStmt)
      else using(conn.prepareStatement(deleteSql(rowSet.size))) { stmt =>
        doFlush(stmt)
      }
      rowSet.clear()
    }

    def delete(sid: RowId) {
      rowSet += sid
      if(rowSet.size == batchSize) flush()
    }

    def finish(): Long = {
      if(rowSet.nonEmpty) flush()
      count
    }

    def close() {
      if(_fullStmt != null) _fullStmt.close()
    }
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

  lazy val findRowsPrefix = "SELECT " + repSchema.values.flatMap(_.physColumns).mkString(",") + " FROM " + dataTableName + " WHERE "

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

  lazy val findSystemIdsPrefix = "SELECT " + sidRep.physColumns.mkString(",") + "," + pkRep.physColumns.mkString(",") + " FROM " + dataTableName + " WHERE "
  def findSystemIds(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[IdPair[CV]]] = {
    require(datasetContext.hasUserPrimaryKey, "findSystemIds called without a user primary key")
    blockQueryById(conn, ids, findSystemIdsPrefix) { rs =>
      val sid = typeContext.makeSystemIdFromValue(sidRep.fromResultSet(rs, 1))
      val uid = pkRep.fromResultSet(rs, 1 + sidRep.physColumns.length)
      IdPair(sid, uid)
    }
  }

  lazy val collectSystemIdsPrefix = "SELECT " + sidRep.physColumns.mkString(",") + " FROM " + dataTableName + " WHERE "
  def collectSystemIds(conn: Connection, ids: Iterator[RowId]): CloseableIterator[RowIdSet] = {
    class ResultIterator extends CloseableIterator[RowIdSet] {
      val prefix = collectSystemIdsPrefix
      val blockSize = 100
      val grouped = new FastGroupedIterator(ids, blockSize)
      var _fullStmt: PreparedStatement = null

      def fullStmt = {
        if(_fullStmt == null) _fullStmt = conn.prepareStatement(prefix + sidRep.templateForMultiLookup(blockSize))
        _fullStmt
      }

      def close() {
        if(_fullStmt != null) _fullStmt.close()
      }

      def hasNext = grouped.hasNext

      def next(): RowIdSet = {
        val block = grouped.next()
        if(block.size != blockSize) {
          using(conn.prepareStatement(prefix + sidRep.templateForMultiLookup(block.size))) { stmt2 =>
            fillAndResult(stmt2, block)
          }
        } else {
          fillAndResult(fullStmt, block)
        }
      }

      def fillAndResult(stmt: PreparedStatement, block: Seq[RowId]): RowIdSet = {
        var i = 1
        for(id <- block) {
          i = sidRep.prepareMultiLookup(stmt, typeContext.makeValueFromSystemId(id), i)
        }
        using(stmt.executeQuery()) { rs =>
          val result = MutableRowIdSet()
          while(rs.next()) {
            result += typeContext.makeSystemIdFromValue(sidRep.fromResultSet(rs, 1))
          }
          result.freeze()
        }
      }
    }
    new ResultIterator with LeakDetect
  }
}
