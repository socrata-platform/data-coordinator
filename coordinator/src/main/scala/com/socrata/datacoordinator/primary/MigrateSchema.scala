package com.socrata.datacoordinator.primary

import java.util.NoSuchElementException
import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation

/**
 * This object takes Liquibase operations and performs according migrations to the truth store schema.
 */
object MigrateSchema extends App {

  /**
   * Performs a Liquibase schema migration.
   * @param args(0) Migration operation to perform.
   * */
  override def main(args: Array[String]) {
    // Verify that one argument was passed
    if (args.length != 1)
      throw new IllegalArgumentException(
        s"Incorrect number of arguments - expected 1 but received ${args.length}")

    // Verify that the argument provided is actually a valid operation
    val operation = {
      try
        MigrationOperation.withName(args(0).toLowerCase.capitalize)
      catch {
        case ex: NoSuchElementException =>
          throw new IllegalArgumentException(
            s"No such migration operation: ${args(0)}. " +
            s"Available operations are [${MigrationOperation.values.mkString(", ")}]")
      }
    }

    SchemaMigrator(databaseTree, operation)
  }
  private lazy val databaseTree = "com.socrata.coordinator.common.database"
}
