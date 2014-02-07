package com.socrata.datacoordinator.truth.migration

import liquibase.Liquibase
import liquibase.database.jvm.JdbcConnection
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
  def migrateDb(conn: Connection, operation: MigrationOperation, changeLogPath: String, database: String) {
    val liquibase = new Liquibase(changeLogPath, new ClassLoaderResourceAccessor, new JdbcConnection(conn))

    operation match {
      case Migrate => liquibase.update(database)
      case Undo => liquibase.rollback(1, database)
      case Redo => { liquibase.rollback(1, database); liquibase.update(database) }
    }
  }
}