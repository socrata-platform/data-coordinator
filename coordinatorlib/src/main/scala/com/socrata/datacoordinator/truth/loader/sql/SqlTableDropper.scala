package com.socrata.datacoordinator.truth.loader
package sql

import java.io.Closeable
import java.sql.Connection

import com.rojoma.simplearm.v2._

class SqlTableDropper(conn: Connection) extends Closeable with TableDropper {
  def close() {
  }

  def go() {
  }

  def scheduleForDropping(tableName: String) {
    using(conn.prepareStatement("INSERT INTO pending_table_drops (table_name, queued_at) values (?, now())")) { stmt =>
      stmt.setString(1, tableName)
      stmt.execute()
    }
  }
}
