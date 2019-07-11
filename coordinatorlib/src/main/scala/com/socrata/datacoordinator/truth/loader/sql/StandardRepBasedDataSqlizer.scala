package com.socrata.datacoordinator
package truth.loader.sql

import java.io.Closeable
import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.DatasetContext
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlColumnRep}
import com.socrata.datacoordinator.id.RowId

class StandardRepBasedDataSqlizer[CT, CV](tableName: String,
                                          datasetContext: RepBasedSqlDatasetContext[CT, CV])
  extends AbstractRepBasedDataSqlizer(tableName, datasetContext)
{
  def insertBatch[T](conn: Connection)(f: Inserter => T): (Long, T) = {
    using(new InserterImpl(conn)) { inserter =>
      val fResult = f(inserter)
      (inserter.stmt.executeBatch().sum, fResult)
    }
  }

  val bulkInsertStatement =
    "INSERT INTO " + dataTableName + " (" + repSchema.iterator.flatMap(_._2.physColumns).mkString(",") + ") VALUES (" + repSchema.iterator.map(_._2.templateForInsert).mkString(",") + ")"

  class InserterImpl(conn: Connection) extends Inserter with Closeable {
    val stmt = conn.prepareStatement(bulkInsertStatement)
    def insert(row: Row[CV]) {
      var i = 1
      val it = repSchema.iterator
      while(it.hasNext) {
        it.advance()
        val k = it.key
        val rep = it.value
        i = rep.prepareInsert(stmt, row.getOrElse(k, typeContext.nullValue), i)
      }
      stmt.addBatch()
    }

    def close() {
      stmt.close()
    }
  }

  def updateBatch[T](conn: Connection)(f: Updater => T): (Long, T) = {
    using(new UpdaterImpl(conn)) { updater =>
      val fResult = f(updater)
      (updater.stmt.executeBatch().sum, fResult)
    }
  }

  val bulkUpdateStatement = "UPDATE " + dataTableName + " SET " + repSchema.values.map(_.templateForUpdate).mkString(",") + " WHERE " + sidRep.templateForSingleLookup

  class UpdaterImpl(conn: Connection) extends Updater with Closeable {
    val stmt = conn.prepareStatement(bulkUpdateStatement)
    def update(sid: RowId, row: Row[CV]) {
      var i = 1
      repSchema.foreach { (cid, rep) =>
        val v = row.getOrElseStrict(cid, typeContext.nullValue)
        i = rep.prepareUpdate(stmt, v, i)
      }
      sidRep.prepareUpdate(stmt, typeContext.makeValueFromSystemId(sid), i)
      stmt.addBatch()
    }

    def close() {
      stmt.close()
    }
  }

  type PreloadStatistics = Unit
  def computeStatistics(conn: Connection): PreloadStatistics = ()
  def updateStatistics(conn: Connection, rowsAdded: Long, rowsDeleted: Long, rowsChanged: Long, preload: PreloadStatistics) {}
}
