package com.socrata.datacoordinator.truth.loader.sql

import java.io.Closeable
import java.sql.Connection

class SqlTableDropper(conn: Connection) extends Closeable {
  //TODO: Store system_id in pending_table_drops so that we can figure out which metadata to delete with the dataset.
  val stmt = conn.prepareStatement("INSERT INTO pending_table_drops (table_name, queued_at) values (?, now())")

  def close() {
    stmt.close()
  }

  def go() {
    stmt.executeBatch()
  }

  def scheduleForDropping(tableName: String) {
    stmt.setString(1, tableName)
    stmt.addBatch()
  }

}
