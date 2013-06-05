package com.socrata.datacoordinator
package truth.loader.sql

import java.sql.Connection

import org.postgresql.PGConnection

import com.socrata.datacoordinator.truth.sql.RepBasedSqlDatasetContext
import com.socrata.datacoordinator.util.ReaderWriterPair
import java.util.concurrent.{Callable, ExecutorService}
import java.io.Reader

class PostgresRepBasedDataSqlizer[CT, CV](tableName: String,
                                          datasetContext: RepBasedSqlDatasetContext[CT, CV],
                                          executor: ExecutorService,
                                          copyIn: (Connection, String, Reader) => Long = PostgresRepBasedDataSqlizer.pgCopyManager)
  extends AbstractRepBasedDataSqlizer(tableName, datasetContext)
{
  val bulkInsertStatement =
    "COPY " + dataTableName + " (" + repSchema.values.flatMap(_.physColumns).mkString(",") + ") from stdin with csv"

  def insertBatch[T](conn: Connection)(f: (Inserter) => T): (Long, T) = {
    val inserter = new InserterImpl
    val result = executor.submit(new Callable[Long] {
      def call() =
        try { copyIn(conn, bulkInsertStatement, inserter.rw.reader) }
        finally { inserter.rw.reader.close() }
    })
    try {
      val fResult = try {
        f(inserter)
      } finally {
        inserter.rw.writer.close()
      }
      (result.get(), fResult)
    } catch {
      case e: Throwable =>
        // Ensure the future has completed before we leave this method.
        // We don't actually care if it failed, because we're exiting
        // abnormally.
        try { result.get() }
        catch { case e: Exception => /* ok */ }
        throw e
    }
  }

  class InserterImpl extends Inserter {
    val rw = new ReaderWriterPair(100000, 1)
    def insert(row: Row[CV]) {
      val sb = new java.lang.StringBuilder
      var didOne = false
      val it = repSchema.iterator
      while(it.hasNext) {
        val (k,v) = it.next()
        if(didOne) sb.append(',')
        else didOne = true

        val value = row.getOrElseStrict(k, nullValue)
        v.csvifyForInsert(sb, value)
      }
      sb.append('\n')
      rw.writer.append(sb)
    }

    def close() {}
  }
}

object PostgresRepBasedDataSqlizer {
  def pgCopyManager(conn: Connection, sql: String, input: Reader): Long =
    conn.asInstanceOf[PGConnection].getCopyAPI.copyIn(sql, input)
}
