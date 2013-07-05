package com.socrata.datacoordinator.backup

import com.socrata.datacoordinator.common.DatabaseCreator

object CreateDatabase extends App {
  DatabaseCreator("com.socrata.coordinator.backup.receiver.database")
}
