package com.socrata.datacoordinator.truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata.CopyInfo

class SqlTruncator(conn: Connection) extends Truncator {
  def truncate[CV](table: CopyInfo, logger: Logger[CV]) {
    using(conn.createStatement()) { stmt =>
      stmt.execute("DELETE FROM " + table.dataTableName)
    }
    logger.truncated()
  }
}
