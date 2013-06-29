package com.socrata.datacoordinator.common

import com.typesafe.config.Config
import javax.sql.DataSource
import java.sql.Connection
import java.io.{OutputStream, Reader}
import org.postgresql.ds.PGSimpleDataSource
import org.postgresql.PGConnection
import com.socrata.datacoordinator.truth.loader.sql.PostgresRepBasedDataSqlizer

class DataSourceConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s
  val host = config.getString(k("host"))
  val port = config.getInt(k("port"))
  val database = config.getString(k("database"))
  val username = config.getString(k("username"))
  val password = config.getString(k("password"))
}

object DataSourceFromConfig {
  def apply(config: DataSourceConfig): (DataSource, (Connection, String, OutputStream => Unit) => Long) = {
    val dataSource = new PGSimpleDataSource
    dataSource.setServerName(config.host)
    dataSource.setPortNumber(config.port)
    dataSource.setDatabaseName(config.database)
    dataSource.setUser(config.username)
    dataSource.setPassword(config.password)
    (dataSource, PostgresRepBasedDataSqlizer.pgCopyManager)
  }
}
