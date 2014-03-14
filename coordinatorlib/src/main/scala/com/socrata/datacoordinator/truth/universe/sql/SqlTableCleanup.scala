package com.socrata.datacoordinator.truth.universe
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

class SqlTableCleanup(conn: Connection, daysDelay: Int = 1) extends TableCleanup {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlTableCleanup])
  def cleanupPendingDrops(): Boolean = {
    using(conn.createStatement()) { stmt =>
      using(stmt.executeQuery(s"SELECT id, table_name FROM pending_table_drops WHERE queued_at < now() - ('$daysDelay day' :: INTERVAL) ORDER BY queued_at LIMIT 1 FOR UPDATE")) { rs =>
        if(rs.next()) {
          val id = rs.getLong("id")
          val tableName = rs.getString("table_name")
          log.info("Physically dropping table " + tableName)
          stmt.execute("DROP TABLE IF EXISTS " + tableName)
          using(conn.prepareStatement("DELETE FROM pending_table_drops WHERE id = ?")) { delStmt =>
            delStmt.setLong(1, id)
            delStmt.execute()
          }
          true
        } else {
          false
        }
      }
    }
  }
}
