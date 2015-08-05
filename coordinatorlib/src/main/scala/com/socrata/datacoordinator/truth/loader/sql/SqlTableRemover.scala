package com.socrata.datacoordinator.truth.loader.sql.SqlTableRemover

import java.io.Closeable
import java.sql.Connection

import com.rojoma.simplearm.util._

/**
 * Created by nathaliek on 8/3/15.
 */
class SqlTableRemover(conn: Connection) extends Closeable {
 
val stmt = conn.createStatement()

  def close() {
    stmt.close()
  }

  def go() {
    stmt.executeBatch()
  }

  def delete(tableName: String) {
      stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName)
  }

}
