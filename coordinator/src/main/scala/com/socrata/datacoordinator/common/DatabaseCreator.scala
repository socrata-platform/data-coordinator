package com.socrata.datacoordinator.common

import com.typesafe.config.ConfigFactory
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.migration.Migration

object DatabaseCreator {
  def apply(databaseTree: String): Unit = {
    val config = ConfigFactory.load()
    println(config.root.render)

    for {
      dsInfo <- DataSourceFromConfig(new DataSourceConfig(config, databaseTree))
      conn <- managed(dsInfo.dataSource.getConnection)
    } {
      Migration.migrateDb(conn)
    }
  }
}
