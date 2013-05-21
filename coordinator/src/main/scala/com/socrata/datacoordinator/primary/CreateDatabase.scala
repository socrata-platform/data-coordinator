package com.socrata.datacoordinator.primary

import com.typesafe.config.ConfigFactory
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.common.{DataSourceConfig, StandardDatasetMapLimits, DataSourceFromConfig}
import com.socrata.datacoordinator.truth.sql.DatabasePopulator

object CreateDatabase extends App {
  val config = ConfigFactory.load()
  println(config.root.render)
  val commonConfig = ConfigFactory.parseString("com.socrata.common.database = ${com.socrata.common.primary-database}").withFallback(config).resolve()
  val root = "com.socrata.common"

  val (dataSource, _) = DataSourceFromConfig(new DataSourceConfig(commonConfig, root + ".database"))

  using(dataSource.getConnection()) { conn =>
    DatabasePopulator.populate(conn, StandardDatasetMapLimits)
  }
}
