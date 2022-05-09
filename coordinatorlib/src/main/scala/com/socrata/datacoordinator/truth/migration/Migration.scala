package com.socrata.datacoordinator.truth.migration

import liquibase.Liquibase
import liquibase.lockservice.LockService
import liquibase.resource.ClassLoaderResourceAccessor

import java.io.OutputStreamWriter
import java.sql.Connection


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
                changeLogPath: String = MigrationScriptPath,
                dryRun: Boolean = false ) {
    val jdbc = new NonCommmittingJdbcConnenction(conn)
    val liquibase = new Liquibase(changeLogPath, new ClassLoaderResourceAccessor, jdbc)
    val lockService = LockService.getInstance(liquibase.getDatabase)
    lockService.setChangeLogLockWaitTime(1000 * 3) // 3s where value should be < SQL lock_timeout (30s)
    val database = conn.getCatalog

    operation match {
      case Migrate =>
        if (dryRun) liquibase.update(database, new OutputStreamWriter(System.out))
        else liquibase.update(database)
      case Undo => liquibase.rollback(numChanges, database)
      case Redo => { liquibase.rollback(numChanges, database); liquibase.update(database) }
    }
    jdbc.realCommit()
  }

  private val MigrationScriptPath = "com.socrata.datacoordinator.truth.schema/migrate.xml"
}
