package com.socrata.datacoordinator.truth.migration

import liquibase.Liquibase
import liquibase.database.jvm.JdbcConnection
import liquibase.logging.LogFactory
import liquibase.resource.ClassLoaderResourceAccessor

import java.sql.Connection

import scala.Enumeration

/**
 * Interface with the Liquibase library to perform schema migrations on a given database with a given set of changes.
 */
object Migration {

  object MigrationOperation extends Enumeration {
    type MigrationOperation = Value
    val Migrate, Undo, Redo = Value
  }
  import MigrationOperation._

  /**
   * Performs a Liquibase schema migration operation on a given database.
   */
  def migrateDb(conn: Connection,
                operation: MigrationOperation = MigrationOperation.Migrate,
                numChanges: Int = 1,
                changeLogPath: String = MigrationScriptPath) {
    val liquibase = new Liquibase(changeLogPath, new ClassLoaderResourceAccessor, new JdbcConnection(conn))
    val database = conn.getCatalog

    operation match {
      case Migrate => liquibase.update(database)
      case Undo => liquibase.rollback(numChanges, database)
      case Redo => { liquibase.rollback(numChanges, database); liquibase.update(database) }
    }
  }

  private val MigrationScriptPath = "com.socrata.datacoordinator.truth.schema/migrate.xml"
}
