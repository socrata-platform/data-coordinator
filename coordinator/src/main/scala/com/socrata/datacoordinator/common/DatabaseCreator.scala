package com.socrata.datacoordinator.common

import com.typesafe.config.ConfigFactory
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.sql.DatabasePopulator

object DatabaseCreator {
  def apply(databaseTree: String) {
    val config = ConfigFactory.load()
    println(config.root.render)

    val (dataSource, _) = DataSourceFromConfig(new DataSourceConfig(config, databaseTree))

    using(dataSource.getConnection()) { conn =>
      DatabasePopulator.populate(conn, StandardDatasetMapLimits)
    }
  }
}
