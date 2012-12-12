package com.socrata.datacoordinator
package truth.loader.sql

import java.sql.{Connection, PreparedStatement}

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.{TypeContext, DatasetContext}
import com.socrata.datacoordinator.truth.sql.{SqlPKableColumnRep, SqlColumnRep}
import com.socrata.datacoordinator.util.{CloseableIterator, FastGroupedIterator, LeakDetect}
import com.socrata.datacoordinator.util.collection.LongLikeMap

abstract class AbstractRepBasedDataSqlizer[CT, CV](tableBase: String,
                                                   val datasetContext: DatasetContext[CT, CV],
                                                   val typeContext: TypeContext[CT, CV],
                                                   repSchemaBuilder: LongLikeMap[ColumnId, CT] => LongLikeMap[ColumnId, SqlColumnRep[CT, CV]])
  extends DataSqlizer[CT, CV]
{
  val nullValue = typeContext.nullValue

  val logicalPKColumnName = datasetContext.primaryKeyColumn

  // map from column IDs to the corresponding rep
  val repSchema = repSchemaBuilder(datasetContext.fullSchema)

  val sidRep = repSchema.get(datasetContext.systemIdColumn).asInstanceOf[SqlPKableColumnRep[CT, CV]]
  val pkRep = repSchema.get(logicalPKColumnName).asInstanceOf[SqlPKableColumnRep[CT, CV]]

  def dataTableName: String = tableBase + "_data"
  def logTableName: String = tableBase + "_log"

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

  def prepareSystemIdDelete(stmt: PreparedStatement, sid: Long) {
    sidRep.prepareSingleLookup(stmt, typeContext.makeValueFromSystemId(sid), 1)
  }

  def sqlizeSystemIdUpdate(sid: Long, row: Row[CV]) = {
    updatePrefix(row).append(sidRep sql_== typeContext.makeValueFromSystemId(sid)).toString
  }

  def updatePrefix(row: Row[CV]) = {
    val sb = new java.lang.StringBuilder("UPDATE ").append(dataTableName).append(" SET ")
    var didOne = false
    val it = row.iterator
    while(it.hasNext) {
      val (k,v) = it.next()

      if(didOne) sb.append(',')
      else didOne = true

      val rep = repSchema(k)
      rep.SETsForUpdate(sb, v)
    }
    sb.append(" WHERE ")
    sb
  }

  val prepareUserIdDeleteStatement =
    "DELETE FROM " + dataTableName + " WHERE " + pkRep.templateForSingleLookup

  def prepareUserIdDelete(stmt: PreparedStatement, id: CV) {
    pkRep.prepareSingleLookup(stmt, id, 1)
  }

  def sqlizeUserIdUpdate(row: Row[CV]) =
    updatePrefix(row).append(pkRep sql_== row(logicalPKColumnName)).toString

  val findSystemIdsPrefix = "SELECT " + sidRep.physColumnsForQuery.mkString(",") + "," + pkRep.physColumnsForQuery.mkString(",") + " WHERE "

  def findSystemIds(conn: Connection, ids: Iterator[CV]): CloseableIterator[Seq[IdPair[CV]]] = {
    require(datasetContext.hasUserPrimaryKey, "findSystemIds called without a user primary key")
    class SystemIdIterator extends CloseableIterator[Seq[IdPair[CV]]] {
      val blockSize = 100
      val grouped = new FastGroupedIterator(ids, blockSize)
      val stmt = conn.prepareStatement(findSystemIdsPrefix + pkRep.templateForMultiLookup(blockSize))

      def hasNext = grouped.hasNext

      def next(): Seq[IdPair[CV]] = {
        val block = grouped.next()
        if(block.size != blockSize) {
          using(conn.prepareStatement(findSystemIdsPrefix + pkRep.templateForMultiLookup(block.size))) { stmt2 =>
            fillAndResult(stmt2, block)
          }
        } else {
          fillAndResult(stmt, block)
        }
      }

      def fillAndResult(stmt: PreparedStatement, block: Seq[CV]): Seq[IdPair[CV]] = {
        var i = 1
        for(cv <- block) {
          i = pkRep.prepareMultiLookup(stmt, cv, i)
        }
        using(stmt.executeQuery()) { rs =>
          val result = new scala.collection.mutable.ArrayBuffer[IdPair[CV]](block.length)
          while(rs.next()) {
            val sid = typeContext.makeSystemIdFromValue(sidRep.fromResultSet(rs, 1))
            val uid = pkRep.fromResultSet(rs, 1 + sidRep.physColumnsForQuery.length)
            result += IdPair(sid, uid)
          }
          result
        }
      }

      def close() { stmt.close() }
    }
    new SystemIdIterator with LeakDetect
  }
}
