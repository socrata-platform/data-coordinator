package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.common.DatabaseCreator

object CreateDatabase extends App {
  DatabaseCreator("com.socrata.coordinator.common.database")
}
