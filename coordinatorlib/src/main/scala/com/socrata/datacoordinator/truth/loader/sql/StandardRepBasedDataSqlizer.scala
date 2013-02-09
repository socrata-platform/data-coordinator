package com.socrata.datacoordinator
package truth.loader.sql

import java.io.Closeable
import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.DatasetContext
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlColumnRep}

class StandardRepBasedDataSqlizer[CT, CV](tableName: String,
                                          datasetContext: RepBasedSqlDatasetContext[CT, CV])
  extends AbstractRepBasedDataSqlizer(tableName, datasetContext)
{
  def insertBatch(conn: Connection)(f: Inserter => Unit): Long = {
    using(new InserterImpl(conn)) { inserter =>
      f(inserter)
      inserter.stmt.executeBatch().foldLeft(0L)(_+_)
    }
  }

  val bulkInsertStatement =
    "INSERT INTO " + dataTableName + " (" + repSchema.iterator.flatMap(_._2.physColumns).mkString(",") + ") VALUES (" + repSchema.iterator.flatMap(_._2.physColumns.map(_ => "?")).mkString(",") + ")"

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
}
