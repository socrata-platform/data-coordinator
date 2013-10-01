package com.socrata.datacoordinator.common

import com.typesafe.config.{ConfigException, Config}
import javax.sql.DataSource
import java.sql.Connection
import java.io.OutputStream
import org.postgresql.ds.PGSimpleDataSource
import com.socrata.datacoordinator.truth.universe.sql.{PostgresCopyIn, C3P0WrappedPostgresCopyIn}
import com.socrata.thirdparty.typesafeconfig.{Propertizer, ConfigClass}
import com.mchange.v2.c3p0.DataSources

class DataSourceConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val host = getString("host")
  val port = getInt("port")
  val database = getString("database")
  val username = getString("username")
  val password = getString("password")
  val applicationName = getString("app-name")
  val poolOptions = optionally(getRawConfig("c3p0")) // these are the c3p0 configuration properties
}

object DataSourceFromConfig {
  def apply(config: DataSourceConfig): (DataSource, (Connection, String, OutputStream => Unit) => Long) = {
    val dataSource = new PGSimpleDataSource
    dataSource.setServerName(config.host)
    dataSource.setPortNumber(config.port)
    dataSource.setDatabaseName(config.database)
    dataSource.setUser(config.username)
    dataSource.setPassword(config.password)
    dataSource.setApplicationName(config.applicationName)
    config.poolOptions match {
      case Some(poolOptions) =>
        val overrideProps = Propertizer("", poolOptions)
        val pooled = DataSources.pooledDataSource(dataSource, null, overrideProps)
        (pooled, C3P0WrappedPostgresCopyIn)
      case None =>
        (dataSource, PostgresCopyIn)
    }
  }
}
