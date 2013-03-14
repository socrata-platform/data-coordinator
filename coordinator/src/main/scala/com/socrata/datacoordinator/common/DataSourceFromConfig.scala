package com.socrata.datacoordinator.common

import com.typesafe.config.Config
import javax.sql.DataSource
import java.sql.Connection
import java.io.Reader
import org.postgresql.ds.PGSimpleDataSource
import org.postgresql.PGConnection

object DataSourceFromConfig {
  def apply(config: Config): (DataSource, (Connection, String, Reader) => Long) = {
    val dataSource = new PGSimpleDataSource
    dataSource.setServerName(config.getString("database.host"))
    dataSource.setPortNumber(config.getInt("database.port"))
    dataSource.setDatabaseName(config.getString("database.database"))
    dataSource.setUser(config.getString("database.username"))
    dataSource.setPassword(config.getString("database.password"))
    (dataSource, pgCopyIn)
  }

  private def pgCopyIn(conn: Connection, sql: String, reader: Reader) =
    conn.asInstanceOf[PGConnection].getCopyAPI.copyIn(sql, reader)
}
