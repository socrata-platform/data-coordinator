package com.socrata.datacoordinator
package truth.loader.sql

import java.sql.{ResultSet, Connection, PreparedStatement}

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlPKableColumnRep, SqlColumnRep}
import com.socrata.datacoordinator.util.{CloseableIterator, FastGroupedIterator, LeakDetect}
import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, RowIdSet, MutableRowIdSet}
import java.io.Closeable

/**
 * Abstract SQL adapter for data access
 *
 * @param softMaxBatchSize in bytes, the max size of each batch to insert.
 * This is actually very tricky because the size is estimated from the SqlRep's estimatedSize
 * method for each column value, but doesn't include the intermediate objects produced, which
 * are actually the most expensive and may take 100x as much memory as the estimated size.
 */
abstract class AbstractRepBasedDataSqlizer[CT, CV](val dataTableName: String,
                                                   val datasetContext: RepBasedSqlDatasetContext[CT, CV],
                                                   val softMaxBatchSize: Int = 1000000)
  extends DataSqlizer[CT, CV]
{
  val typeContext = datasetContext.typeContext

  val nullValue = typeContext.nullValue

  val logicalPKColumnName = datasetContext.primaryKeyColumn

  val repSchema = datasetContext.schema

  val sidRep = repSchema(datasetContext.systemIdColumn).asInstanceOf[SqlPKableColumnRep[CT, CV]]
  val pkRep = repSchema(logicalPKColumnName).asInstanceOf[SqlPKableColumnRep[CT, CV]]
  val versionRep = repSchema(datasetContext.versionColumn)

  def sizeofDelete(id: CV) = pkRep.estimateSize(id)

  def sizeof(row: Row[CV]) = {
    var total = 0
    val it = row.iterator
    while(it.hasNext) {
      it.advance()
      total += repSchema(it.key).estimateSize(it.value)
    }
    total
  }

  def deleteBatch[T](conn: Connection)(f: Deleter => T): (Long, T) = {
    using(new DeleterImpl(conn)) { deleter =>
      val fResult = f(deleter)
      (deleter.finish(), fResult)
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
    "UPDATE " + dataTableName + " SET " + repSchema.values.map(_.templateForUpdate).mkString(",") + " WHERE " + sidRep.templateForSingleLookup

  def prepareSystemIdUpdate(stmt: PreparedStatement, sid: RowId, row: Row[CV]) {
    var i = 1
    repSchema.foreach { (cid, rep) =>
      val v = row.getOrElseStrict(cid, typeContext.nullValue)
      i = rep.prepareUpdate(stmt, v, i)
    }
    sidRep.prepareUpdate(stmt, typeContext.makeValueFromSystemId(sid), i)
  }

  def blockQueryById[T](
    conn: Connection,
    ids: Iterator[CV],
    selectReps: Iterable[SqlColumnRep[CT, CV]],
    dataTableName: String)
      (decode: ResultSet => T): CloseableIterator[Seq[T]] = {

    class ResultIterator extends CloseableIterator[Seq[T]] {
      val blockSize = 100
      val grouped = new FastGroupedIterator(ids, blockSize)
      val prefix = s"SELECT ${selectReps.map(_.selectList).mkString(",")} FROM $dataTableName WHERE "
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

  def findRows(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[InspectedRow[CV]]] =
    blockQueryById(conn, ids, repSchema.values, dataTableName) { rs =>
      val row = new MutableRow[CV]
      var i = 1
      repSchema.foreach { (cid, rep) =>
        row(cid) = rep.fromResultSet(rs, i)
        i += rep.physColumns.length
      }
      val id = row(datasetContext.primaryKeyColumn)
      val sid = row(datasetContext.systemIdColumn)
      val version = typeContext.makeRowVersionFromValue(row(datasetContext.versionColumn))
      InspectedRow(id, typeContext.makeSystemIdFromValue(sid), version, row.freeze())
    }

  def findIdsAndVersions(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[InspectedRowless[CV]]] = {
    def processBlockNoUid(rs: ResultSet) = {
      val uid = pkRep.fromResultSet(rs, 1)
      val sid = typeContext.makeSystemIdFromValue(uid)
      val ver = typeContext.makeRowVersionFromValue(versionRep.fromResultSet(rs, 1 + pkRep.physColumns.length))
      InspectedRowless(uid, sid, ver)
    }
    def processBlockWithUid(rs: ResultSet) = {
      val uid = pkRep.fromResultSet(rs, 1)
      val sid = typeContext.makeSystemIdFromValue(sidRep.fromResultSet(rs, 1 + pkRep.physColumns.length))
      val ver = typeContext.makeRowVersionFromValue(versionRep.fromResultSet(rs, 1 + pkRep.physColumns.length + sidRep.physColumns.length))
      InspectedRowless(uid, sid, ver)
    }
    if(pkRep eq sidRep) {
      blockQueryById(conn, ids, Seq(pkRep, versionRep), dataTableName)(processBlockNoUid)
    } else {
      blockQueryById(conn, ids, Seq(pkRep, sidRep, versionRep), dataTableName)(processBlockWithUid)
    }
  }
}
