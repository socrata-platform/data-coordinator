package com.socrata.datacoordinator.common

import com.typesafe.config.Config
import javax.sql.DataSource
import java.sql.Connection
import java.io.OutputStream
import org.postgresql.core.ProtocolConnection
import org.postgresql.ds.PGSimpleDataSource
import com.socrata.datacoordinator.truth.universe.sql.{PostgresCopyIn, C3P0WrappedPostgresCopyIn}
import com.socrata.thirdparty.typesafeconfig.{C3P0Propertizer, ConfigClass}
import com.mchange.v2.c3p0.{AbstractConnectionCustomizer, DataSources}
import com.rojoma.simplearm.{SimpleArm, Managed}
import org.postgresql.jdbc2.AbstractJdbc2Connection
import org.slf4j.LoggerFactory

class DataSourceConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val host = getString("host")
  val port = getInt("port")
  val database = getString("database")
  val username = getString("username")
  val password = getString("password")
  val applicationName = getString("app-name")
  val tcpKeepAlive = optionally(getBoolean("tcp-keep-alive")).getOrElse(false)
  val poolOptions = optionally(getRawConfig("c3p0")) // these are the c3p0 configuration properties
}

object DataSourceFromConfig {
  case class DSInfo(dataSource: DataSource, copyIn: (Connection, String, OutputStream => Unit) => Long)
  def apply(config: DataSourceConfig): Managed[DSInfo] =
    new SimpleArm[DSInfo] {
      def flatMap[A](f: DSInfo => A): A = {
        val dataSource = new PGSimpleDataSource
        dataSource.setServerName(config.host)
        dataSource.setPortNumber(config.port)
        dataSource.setDatabaseName(config.database)
        dataSource.setUser(config.username)
        dataSource.setPassword(config.password)
        dataSource.setApplicationName(config.applicationName)
        dataSource.setTcpKeepAlive(config.tcpKeepAlive)
        config.poolOptions match {
          case Some(poolOptions) =>
            val overrideProps = C3P0Propertizer("", poolOptions)
            val pooled = DataSources.pooledDataSource(dataSource, null, overrideProps)
            try {
              f(DSInfo(pooled, C3P0WrappedPostgresCopyIn))
            } finally {
              DataSources.destroy(pooled)
            }
          case None =>
            f(DSInfo(dataSource, PostgresCopyIn))
        }
      }
    }
}

// ConnectionCustomizer for resetting transactions back to the default transaction isolation level
class ConnectionResetter extends AbstractConnectionCustomizer {
  val logger = LoggerFactory.getLogger(classOf[ConnectionResetter])

  override def onCheckIn(c: Connection, parentDataSourceIdentityToken: String): Unit = {
    c match {
      case conn: AbstractJdbc2Connection =>
        if (conn.getTransactionState != ProtocolConnection.TRANSACTION_IDLE) {
          try {
            throw new Exception
          } catch {
            case e: Exception =>
              logger.warn("Connection was not idle check-in", e)
          }
          c.rollback()
        }
      case other =>
        logger.warn("Did not get a postgres Jdbc connection on check-in; unconditionally rolling back.")
        c.rollback()
    }
    c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED) // default isolation level for PostgreSQL
  }
}
