package com.socrata.datacoordinator.backup

import com.socrata.datacoordinator.primary.SchemaMigrator
import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation

object CreateDatabase extends App {
  SchemaMigrator("com.socrata.coordinator.backup.receiver.database", MigrationOperation.Migrate)
}
