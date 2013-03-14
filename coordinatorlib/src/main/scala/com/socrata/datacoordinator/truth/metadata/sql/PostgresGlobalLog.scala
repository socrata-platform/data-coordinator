package com.socrata.datacoordinator
package truth.metadata
package sql

import java.sql.{ResultSet, Statement, Timestamp, Connection}

import org.joda.time.DateTime
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.CloseableIterator
import scala.collection.immutable.VectorBuilder
import com.socrata.datacoordinator.id.{DatasetId, GlobalLogEntryId}

class PostgresGlobalLog(conn: Connection) extends GlobalLog {
  def log(tableInfo: DatasetInfo, version: Long, updatedAt: DateTime, updatedBy: String) {
    // bit heavyweight but we want an absolute ordering on these log entries.  In particular,
    // we never want row with id n+1 to become visible to outsiders before row n, even ignoring
    // any other problems.  This is the reason for the "this should be the last thing a txn does"
    // note on the interface.
    using(conn.createStatement()) { stmt =>
      stmt.execute("LOCK TABLE global_log IN EXCLUSIVE MODE")
    }

    using(conn.prepareStatement("INSERT INTO global_log (id, dataset_system_id, version, updated_at, updated_by) SELECT COALESCE(max(id), 0) + 1, ?, ?, ?, ? FROM global_log")) { stmt =>
      stmt.setLong(1, tableInfo.systemId.underlying)
      stmt.setLong(2, version)
      stmt.setTimestamp(3, new Timestamp(updatedAt.getMillis))
      stmt.setString(4, updatedBy)
      val count = stmt.executeUpdate()
      assert(count == 1, "Insert into global_log didn't create a row?")
    }
  }
}

class PostgresGlobalLogPlayback(conn: Connection, blockSize: Int = 500, forBackup: Boolean = true) extends GlobalLogPlayback {
  private val table = if(forBackup) "last_id_sent_to_backup" else "last_id_processed_for_secondaries"
  def pendingJobs(): Iterator[Job] = {
    def loop(lastBlock: Vector[Job]): Stream[Vector[Job]] = {
      if(lastBlock.isEmpty) Stream.empty
      else {
        val newBlock = nextBlock(lastBlock.last.id)
        newBlock #:: loop(newBlock)
      }
    }
    val first = firstBlock()
    (first #:: loop(first)).iterator.flatten
  }

  def firstBlock(): Vector[Job] =
    for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery(s"select id, dataset_system_id, version from global_log where id > (select coalesce(max(id), 0) from $table}) order by id limit " + blockSize))
    } yield resultOfQuery(rs)

  def nextBlock(lastId: GlobalLogEntryId) =
    for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery("select id, dataset_system_id, version from global_log where id > " + lastId.underlying + " order by id limit " + blockSize))
    } yield resultOfQuery(rs)

  def resultOfQuery(rs: ResultSet): Vector[Job] = {
    val result = new VectorBuilder[Job]
    while(rs.next()) {
      result += Job(
        new GlobalLogEntryId(rs.getLong("id")),
        new DatasetId(rs.getLong("dataset_system_id")),
        rs.getLong("version")
      )
    }
    result.result()
  }

  def finishedJob(job: Job) {
    using(conn.createStatement()) { stmt =>
      if(stmt.executeUpdate(s"UPDATE $table SET id = " + job.id.underlying) == 0) {
        stmt.executeUpdate(s"INSERT INTO $table (id) VALUES (" + job.id.underlying + ")")
      }
    }
  }
}
