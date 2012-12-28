package com.socrata.datacoordinator
package truth.metadata
package sql

import java.sql.{Timestamp, Connection}

import org.joda.time.DateTime
import com.rojoma.simplearm.util._

class PostgresGlobalLog(conn: Connection) extends GlobalLog {
  def log(tableInfo: DatasetMapWriter#DatasetInfo, version: Long, updatedAt: DateTime, updatedBy: String) {
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
