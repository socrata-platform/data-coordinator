package com.socrata.datacoordinator
package truth.loader.sql

import java.sql.{Connection, PreparedStatement}

import org.postgresql.PGConnection

import com.socrata.datacoordinator.truth.sql.RepBasedSqlDatasetContext
import com.socrata.datacoordinator.id.RowId
import java.io.{OutputStreamWriter, BufferedWriter, OutputStream}
import java.nio.charset.StandardCharsets
import org.postgresql.copy.CopyIn
import com.rojoma.simplearm.v2._

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
      while (it.hasNext) {
        val (k, v) = it.next()
        if (didOne) sb.append(',')
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

  def updateBatch[T](conn: Connection)(f: Updater => T): (Long, T) = {
    val updater = new UpdaterImpl(conn)
    val fResult = f(updater)
    (updater.finish(), fResult)
  }


  // We're constructing a statement that looks like
  //  UPDATE t12345
  //    SET c1 = the_rows.c1, c2 = the_rows.c2, ...
  //    FROM (VALUES
  //      (v1a, v2a, ...),
  //      (v1b, v2b, ...),
  //      ...
  //    ) AS the_rows(c1, c2, ...)
  //    WHERE t12345.sid = the_rows.sid

  private val updaterPfx = "UPDATE " + dataTableName + " SET " + repSchema.values.map(_.physColumns.map { c => c + "=the_rows." + c }.mkString(",")).mkString(",") + " FROM (VALUES "
  private val updaterRowValues = repSchema.values.map(_.templateForInsert).mkString("(", ",", ")")
  private val updaterSfx = ") AS the_rows(" + repSchema.values.flatMap(_.physColumns).mkString(",") + ") WHERE " + sidRep.physColumns.map { c => dataTableName + "." + c + "=the_rows." + c }.mkString(" AND ")

  class UpdaterImpl(conn: Connection) extends Updater {
    val rows = Vector.newBuilder[Row[CV]]

    def update(sid: RowId, row: Row[CV]) {
      // The sid is only needed by the non-postgres-specific sqlizer
      // in order to build the WHERE clause.  It's duplicated by
      // (some) field in the row.
      rows += row
    }

    def finish(): Long = {
      using(new ResourceScope) { rs =>
        // The maximum prepared statement parameter number is 2¹⁵-1, so we can
        // have 2¹⁵-2 parameters  since they start with 1, not 0.
        val maxParameters = 0x7ffe

        val maxRowsPerStmt = maxParameters / repSchema.values.map(_.physColumns.length).sum

        var lastStmt = Option.empty[PreparedStatement]
        var totalUpdated = 0L

        for(rowBatch <- rows.result().grouped(maxRowsPerStmt)) {
          val stmt = lastStmt match {
            case Some(s) if rowBatch.length == maxRowsPerStmt =>
              s
            case _ =>
              lastStmt.foreach { stmt =>
                totalUpdated += stmt.executeBatch().sum
                rs.close(stmt)
              }

              val sql = Iterator.fill(rowBatch.size)(updaterRowValues).mkString(updaterPfx, ",", updaterSfx)
              rs.open(conn.prepareStatement(sql))
          }

          lastStmt = Some(stmt)

          var i = 1
          for {
            row <- rowBatch
            cidRep <- repSchema
          } {
            val (cid, rep) = cidRep
            val v = row.getOrElseStrict(cid, typeContext.nullValue)
            i = rep.prepareInsert(stmt, v, i)
          }

          stmt.addBatch()
        }

        lastStmt.foreach { stmt =>
          totalUpdated += stmt.executeBatch().sum
        }

        totalUpdated
      }
    }
  }

  case class StatSpec(pgCount: Long, added: Long, deleted: Long)

  type PreloadStatistics = StatSpec

  def computeStatistics(conn: Connection): PreloadStatistics =
    using(conn.prepareStatement("SELECT reltuples FROM pg_class WHERE relname=?")) { stmt =>
      stmt.setString(1, tableName)
      using(stmt.executeQuery()) { rs =>
        if (rs.next()) {
          StatSpec(rs.getLong("reltuples"), 0, 0)
        } else {
          StatSpec(0, 0, 0)
        }
      }
    }

  def updateStatistics(conn: Connection, rowsAdded: Long, rowsDeleted: Long, rowsChanged: Long, preload: PreloadStatistics): PreloadStatistics = {
    // If we have grown and/or shrunk the table by more than about 25%
    // beyond what PG believed the row count to have been (with a
    // minimum of 10000 rows to prevent a flurry of ANALYZEs during
    // the initial growth phase), we should poke the PG statistics for
    // it.
    val totalAdded = preload.added + rowsAdded
    val totalDeleted = preload.deleted + rowsDeleted
    if (totalAdded + totalDeleted >= Math.max(10000, preload.pgCount / 4)) {
      val cols = (sidRep.physColumns ++ pkRep(bySystemIdForced = false).physColumns).toSet
      using(conn.prepareStatement("ANALYZE " + tableName + " (" + cols.mkString(",") + ")")) { stmt =>
        stmt.execute()
      }
      computeStatistics(conn)
    } else {
      preload.copy(added = totalAdded, deleted = totalDeleted)
    }
  }
}

object PostgresRepBasedDataSqlizer {
  def pgCopyManager(conn: Connection, sql: String, output: OutputStream => Unit): Long = {
    val copyIn = conn.asInstanceOf[PGConnection].getCopyAPI.copyIn(sql)
    try {
      output(new CopyInOutputStream(copyIn))
      copyIn.endCopy()
    } finally {
      if(copyIn.isActive) copyIn.cancelCopy()
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
