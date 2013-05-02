package com.socrata.datacoordinator
package truth.metadata
package sql

import java.sql._

import org.joda.time.DateTime
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.CloseableIterator
import scala.collection.immutable.VectorBuilder
import com.socrata.datacoordinator.id.{DatasetId, GlobalLogEntryId}
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.truth.metadata.DatasetInfo

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
      stmt.setDatasetId(1, tableInfo.systemId)
      stmt.setLong(2, version)
      stmt.setTimestamp(3, new Timestamp(updatedAt.getMillis))
      stmt.setString(4, updatedBy)
      val count = stmt.executeUpdate()
      assert(count == 1, "Insert into global_log didn't create a row?")
    }
  }
}
class PostgresBackupPlaybackManifest(conn: Connection) extends PlaybackManifest {
  private val table = "last_id_sent_to_backup"

  def lastJobId(): GlobalLogEntryId = {
    for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery(s"SELECT MAX(id) from $table"))
    } yield {
      rs.next()
      new GlobalLogEntryId(rs.getLong(1)) // max returns null if there are no values, getLong returns 0 on null.
    }
  }

  def finishedJob(job: GlobalLogEntryId) {
    val updateCount = using(conn.prepareStatement(s"UPDATE $table SET id = ?")) { stmt =>
      stmt.setLong(1, job.underlying)
      stmt.executeUpdate()
    }
    if(updateCount == 0) {
      using(conn.prepareStatement(s"INSERT INTO $table (id) VALUES (?)")) { stmt =>
        stmt.setLong(1, job.underlying)
        stmt.execute()
      }
    }
  }
}

class PostgresSecondaryPlaybackManifest(conn: Connection, secondaryId: String) extends PlaybackManifest {
  private val table = "last_id_processed_for_secondaries"

  def lastJobId(): GlobalLogEntryId = {
    using(conn.prepareStatement(s"SELECT MAX(global_log_id) from $table WHERE secondary_id = ?")) { stmt =>
      stmt.setString(1, secondaryId)
      using(stmt.executeQuery()) { rs =>
        rs.next()
        new GlobalLogEntryId(rs.getLong(1))
      }
    }
  }

  def finishedJob(job: GlobalLogEntryId) {
    val updateCount = using(conn.prepareStatement(s"UPDATE $table SET global_log_id = ? WHERE secondary_id = ?")) { stmt =>
      stmt.setLong(1, job.underlying)
      stmt.setString(2, secondaryId)
      stmt.executeUpdate()
    }
    if(updateCount == 0) {
      using(conn.prepareStatement(s"INSERT INTO $table (secondary_id, global_log_id) VALUES (?,?)")) { stmt =>
        stmt.setString(1, secondaryId)
        stmt.setLong(2, job.underlying)
        stmt.execute()
      }
    }
  }
}

class PostgresGlobalLogPlayback(conn: Connection, blockSize: Int = 500) extends GlobalLogPlayback {
  def pendingJobs(aboveJob: GlobalLogEntryId): Iterator[Job] = {
    def loop(lastBlock: Vector[Job]): Stream[Vector[Job]] = {
      if(lastBlock.isEmpty) Stream.empty
      else {
        val newBlock = nextBlock(lastBlock.last.id)
        newBlock #:: loop(newBlock)
      }
    }
    val first = nextBlock(aboveJob)
    (first #:: loop(first)).iterator.flatten
  }

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
        rs.getDatasetId("dataset_system_id"),
        rs.getLong("version")
      )
    }
    result.result()
  }
}
