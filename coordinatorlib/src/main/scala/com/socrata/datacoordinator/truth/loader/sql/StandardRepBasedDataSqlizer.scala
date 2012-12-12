package com.socrata.datacoordinator
package truth.loader.sql

import java.io.Closeable
import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.{TypeContext, DatasetContext}
import com.socrata.datacoordinator.util.collection.LongLikeMap
import com.socrata.datacoordinator.truth.sql.SqlColumnRep

class StandardRepBasedDataSqlizer[CT, CV](tableBase: String,
                                          datasetContext: DatasetContext[CT, CV],
                                          typeContext: TypeContext[CT, CV],
                                          repSchemaBuilder: LongLikeMap[ColumnId, CT] => LongLikeMap[ColumnId, SqlColumnRep[CT, CV]])
  extends AbstractRepBasedDataSqlizer(tableBase, datasetContext, typeContext, repSchemaBuilder)
{
  def insertBatch(conn: Connection)(f: Inserter => Unit): Long = {
    using(new InserterImpl(conn)) { inserter =>
      f(inserter)
      inserter.stmt.executeBatch().foldLeft(0L)(_+_)
    }
  }

  val bulkInsertStatement =
    "INSERT INTO " + dataTableName + " (" + repSchema.iterator.flatMap(_._2.physColumnsForInsert).mkString(",") + ") VALUES (" + repSchema.iterator.flatMap(_._2.physColumnsForInsert.map(_ => "?")).mkString(",") + ")"

  class InserterImpl(conn: Connection) extends Inserter with Closeable {
    val stmt = conn.prepareStatement(bulkInsertStatement)
    def insert(row: Row[CV]) {
      var i = 1
      val it = repSchema.iterator
      while(it.hasNext) {
        it.advance()
        val k = it.key
        val rep = it.value
        i += rep.prepareInsert(stmt, row.getOrElse(k, typeContext.nullValue), i)
      }
      stmt.addBatch()
    }

    def close() {
      stmt.close()
    }
  }
}
