package com.socrata.datacoordinator.common

import com.typesafe.config.Config
import javax.sql.DataSource
import java.sql.Connection
import java.io.Reader
import org.postgresql.ds.PGSimpleDataSource
import org.postgresql.PGConnection

class DataSourceConfig(config: Config) {
  val host = config.getString("host")
  val port = config.getInt("port")
  val database = config.getString("database")
  val username = config.getString("username")
  val password = config.getString("password")
}

object DataSourceFromConfig {
  def apply(config: DataSourceConfig): (DataSource, (Connection, String, Reader) => Long) = {
    val dataSource = new PGSimpleDataSource
    dataSource.setServerName(config.host)
    dataSource.setPortNumber(config.port)
    dataSource.setDatabaseName(config.database)
    dataSource.setUser(config.username)
    dataSource.setPassword(config.password)
    (dataSource, pgCopyIn)
  }

  private def pgCopyIn(conn: Connection, sql: String, reader: Reader) =
    conn.asInstanceOf[PGConnection].getCopyAPI.copyIn(sql, reader)
}
