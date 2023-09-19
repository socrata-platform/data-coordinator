package com.socrata.datacoordinator.common

import com.typesafe.config.Config
import javax.sql.DataSource
import java.sql.Connection
import java.io.{OutputStream, Closeable}
import org.postgresql.ds.PGSimpleDataSource
import com.socrata.datacoordinator.truth.universe.sql.{PostgresCopyIn, C3P0WrappedPostgresCopyIn}
import com.socrata.thirdparty.typesafeconfig.{C3P0Propertizer, ConfigClass}
import com.mchange.v2.c3p0.DataSources
import com.rojoma.simplearm.v2._

class DataSourceConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val dbType  = optionally(getString("db-type"))
    .map(choice =>
      DbType
        .parse(choice)
        .getOrElse(throw new IllegalArgumentException(s"Illegal choice for db-type: $choice"))
    ).getOrElse(Postgres)


  val host = getString("host")
  val port = getInt("port")
  val database = getString("database")
  val username = getString("username")
  val password = getString("password")
  val applicationName = getString("app-name")
  val tcpKeepAlive = optionally(getBoolean("tcp-keep-alive")).getOrElse(false)
  val loginTimeout = optionally(getDuration("login-timeout"))
  val connectTimeout = optionally(getDuration("connect-timeout"))
  val cancelSignalTimeout = optionally(getDuration("cancel-signal-timeout"))
  val poolOptions = optionally(getRawConfig("c3p0")) // these are the c3p0 configuration properties
}

object DataSourceFromConfig {
  trait DSInfo {
    val dataSource: DataSource
    val copyIn: (Connection, String, OutputStream => Unit) => Long
  }

  private def pgDataSource(config: DataSourceConfig): PGSimpleDataSource = {
    val dataSource = new PGSimpleDataSource
    dataSource.setServerName(config.host)
    dataSource.setPortNumber(config.port)
    dataSource.setDatabaseName(config.database)
    dataSource.setUser(config.username)
    dataSource.setPassword(config.password)
    dataSource.setApplicationName(config.applicationName)
    dataSource.setTcpKeepAlive(config.tcpKeepAlive)
    // for these optional configs, we should fall back to the driver's default, if no value is specified
    config.loginTimeout.foreach(t => dataSource.setLoginTimeout(t.toSeconds.toInt))
    config.connectTimeout.foreach(t => dataSource.setConnectTimeout(t.toSeconds.toInt))
    config.cancelSignalTimeout.foreach(t => dataSource.setCancelSignalTimeout(t.toSeconds.toInt))
    dataSource
  }

  def unmanaged(config: DataSourceConfig): DSInfo with Closeable = {
    config.poolOptions match {
      case Some(poolOptions) =>
        new DSInfo with Closeable {
          override val copyIn = C3P0WrappedPostgresCopyIn

          override val dataSource =
            DataSources.pooledDataSource(
              pgDataSource(config),
              null,
              C3P0Propertizer("", poolOptions)
            )

          override def close() {
            DataSources.destroy(dataSource)
          }
        }
      case None => {
        new DSInfo with Closeable {
          override val copyIn = PostgresCopyIn
          override val dataSource = pgDataSource(config)
          override def close() {}
        }
      }
    }
  }

  def apply(config: DataSourceConfig): Managed[DSInfo] =
    new Managed[DSInfo] {
      def run[A](f: DSInfo => A): A = {
        val dsInfo = unmanaged(config)
        try {
          f(dsInfo)
        } finally {
          dsInfo.close()
        }
      }
    }

  def apply(config: DataSourceConfig, rs: ResourceScope): DSInfo =
    rs.open(unmanaged(config))
}
