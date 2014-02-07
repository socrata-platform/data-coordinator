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
  def apply(databaseTree: String, operation: MigrationOperation) {
    val config = ConfigFactory.load
    for {
      dataSourceInfo <- DataSourceFromConfig(new DataSourceConfig(config, databaseTree))
      conn <- managed(dataSourceInfo.dataSource.getConnection)
    } {
      Migration.migrateDb(conn, operation, "schema/migrate.xml", config.getString(s"$databaseTree.database"))
    }
  }
}
