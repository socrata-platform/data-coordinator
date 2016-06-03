package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation.MigrationOperation
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.datacoordinator.truth.migration.Migration

import com.typesafe.config.ConfigFactory
import com.rojoma.simplearm.util._

/**
 * Performs Liquibase migrations on the truth store.
 */
object SchemaMigrator {
  def apply(databaseTree: String, operation: MigrationOperation, numChanges: Int = 1): Unit = {
    val config = ConfigFactory.load
    for {
      dataSourceInfo <- DataSourceFromConfig(new DataSourceConfig(config, databaseTree))
      conn <- managed(dataSourceInfo.dataSource.getConnection)
      stmt <- managed(conn.createStatement())
    } {
      stmt.execute("SET lock_timeout = '30s'")
      Migration.migrateDb(conn, operation, numChanges)
    }
  }
}
