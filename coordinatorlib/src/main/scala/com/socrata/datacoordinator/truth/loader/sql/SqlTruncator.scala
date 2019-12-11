package com.socrata.datacoordinator.truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.v2._

import com.socrata.datacoordinator.truth.metadata.CopyInfo

class SqlTruncator(conn: Connection) extends Truncator {
  def truncate(table: CopyInfo, logger: Logger[_, _]) {
    using(conn.createStatement()) { stmt =>
      stmt.execute("DELETE FROM " + table.dataTableName)
    }
    logger.truncated()
  }
}
