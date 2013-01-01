package com.socrata.datacoordinator
package truth.loader.sql

import java.sql.Connection

import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import com.socrata.datacoordinator.truth.sql.RepBasedSqlDatasetContext
import com.socrata.datacoordinator.util.StringBuilderReader

class PostgresRepBasedDataSqlizer[CT, CV](tableName: String,
                                          datasetContext: RepBasedSqlDatasetContext[CT, CV],
                                          extractCopier: Connection => CopyManager = PostgresRepBasedDataSqlizer.pgCopyManager)
  extends AbstractRepBasedDataSqlizer(tableName, datasetContext)
{
  val bulkInsertStatement =
    "COPY " + dataTableName + " (" + repSchema.values.flatMap(_.physColumnsForInsert).mkString(",") + ") from stdin with csv"

  def insertBatch(conn: Connection)(f: (Inserter) => Unit) = {
    val inserter = new InserterImpl
    f(inserter)
    val copyManager = extractCopier(conn)
    copyManager.copyIn(bulkInsertStatement, inserter.reader)
  }

  class InserterImpl extends Inserter {
    val sb = new java.lang.StringBuilder
    def insert(row: Row[CV]) {
      var didOne = false
      val it = repSchema.iterator
      while(it.hasNext) {
        val (k,v) = it.next()
        if(didOne) sb.append(',')
        else didOne = true

        val value = row.getOrElse(k, nullValue)
        v.csvifyForInsert(sb, value)
      }
      sb.append('\n')
    }

    def close() {}

    def reader: java.io.Reader = new StringBuilderReader(sb)
  }
}

object PostgresRepBasedDataSqlizer {
  def pgCopyManager(conn: Connection) = conn.asInstanceOf[BaseConnection].getCopyAPI
}
