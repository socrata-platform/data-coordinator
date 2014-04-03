package com.socrata.datacoordinator
package truth.loader.sql


import com.mchange.v2.c3p0.C3P0ProxyConnection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.sql.RepBasedSqlDatasetContext
import com.socrata.datacoordinator.truth.universe.sql.C3P0WrappedPostgresCopyIn
import java.io.{OutputStreamWriter, BufferedWriter, OutputStream}
import java.nio.charset.StandardCharsets
import java.sql.Connection
import org.postgresql.copy.CopyIn
import org.postgresql.PGConnection


class PostgresRepBasedDataSqlizer[CT, CV](tableName: String,
                                          datasetContext: RepBasedSqlDatasetContext[CT, CV],
                                          copyIn: (Connection, String, OutputStream => Unit) => Long = PostgresRepBasedDataSqlizer.pgCopyManager)
  extends AbstractRepBasedDataSqlizer(tableName, datasetContext)
{
  val bulkInsertStatement =
    "COPY " + dataTableName + " (" + repSchema.values.flatMap(_.physColumns).mkString(",") + ") from stdin with (format csv, encoding 'utf-8')"

  def insertBatch[T](conn: Connection)(f: (Inserter) => T): (Long, T) = {
    var result: T = null.asInstanceOf[T]
    def writeF(w: OutputStream) {
      val inserter = new InserterImpl(w)
      result = f(inserter)
      inserter.close()
    }
    val count = copyIn(conn, bulkInsertStatement, writeF)
    (count, result)
  }

  class InserterImpl(out: OutputStream) extends Inserter {
    val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
    val sb = new java.lang.StringBuilder

    def insert(row: Row[CV]) {
      sb.setLength(0)
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
      writer.append(sb)
    }

    def close() {
      writer.close()
    }
  }

  type PreloadStatistics = Long

  def computeStatistics(conn: Connection): PreloadStatistics =
    using(conn.prepareStatement("SELECT reltuples FROM pg_class WHERE relname=?")) { stmt =>
      stmt.setString(1, tableName)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          rs.getLong("reltuples")
        } else {
          0L
        }
      }
    }

  def updateStatistics(conn: Connection, rowsAdded: Long, rowsDeleted: Long, rowsChanged: Long, preload: PreloadStatistics) {
    if(rowsAdded + rowsDeleted + rowsChanged >= preload / 10) {
      val cols = (sidRep.physColumns ++ pkRep.physColumns).toSet
      using(conn.prepareStatement("ANALYZE " + tableName + " (" + cols.mkString(",") + ")")) { stmt =>
        stmt.execute()
      }
    }
  }
}

object PostgresRepBasedDataSqlizer {
  def pgCopyManager(conn: Connection, sql: String, output: OutputStream => Unit): Long = {
    conn match {
      case pgConn: PGConnection =>
        val copyIn = pgConn.getCopyAPI.copyIn(sql)
        try {
          output(new CopyInOutputStream(copyIn))
          copyIn.endCopy()
        } finally {
          if(copyIn.isActive) copyIn.cancelCopy()
        }
      case c3p0Conn: C3P0ProxyConnection =>
        C3P0WrappedPostgresCopyIn(conn, sql, output)
      case unknown =>
        throw new Exception("Unknown sql connection " + unknown.getClass.getName)
    }
  }

  class CopyInOutputStream(copyIn: CopyIn) extends OutputStream {
    def write(b: Int) {
      copyIn.writeToCopy(new Array[Byte](b.toByte), 0, 1)
    }

    override def write(bs: Array[Byte], start: Int, length: Int) {
      copyIn.writeToCopy(bs, start, length)
    }
  }
}