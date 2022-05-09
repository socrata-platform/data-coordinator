package com.socrata.datacoordinator.primary

import java.util.NoSuchElementException
import com.socrata.datacoordinator.service.Main
import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation

import scala.sys.exit

/**
 * This object takes Liquibase operations and performs according migrations to the truth store schema.
 */
object MigrateSchema extends App {
  private lazy val databaseTree = s"${Main.configRoot}.database"

  private def exitWithArgumentUsageHelp() = {
    println(s"Incorrect arguments! usage: migrate|undo|redo [$$numChanges] [--dry-run]")
    exit(-1)
  }

  // Verify that one argument was passed
  val (argsMinusDryRun, dryRun) = args.foldLeft((Seq.empty[String], false)) { (acc, a) =>
    val (aa, dryRun) = acc
    if (a == "--dry-run") (aa, true)
    else (aa :+ a, dryRun)
  }

  if(argsMinusDryRun.length < 1 || argsMinusDryRun.length > 2) {
    exitWithArgumentUsageHelp()
  }

  val numChanges = argsMinusDryRun.length match {
    case 1 => 1
    case 2 => try { argsMinusDryRun(1).toInt } catch { case _: NumberFormatException => exitWithArgumentUsageHelp() }
  }

  // Verify that the argument provided is actually a valid operation
  val operation = {
    try {
      MigrationOperation.withName(args(0).toLowerCase.capitalize)
    } catch {
      case ex: NoSuchElementException =>
        throw new IllegalArgumentException(
          s"No such migration operation: ${args(0)}. " +
            s"Available operations are [${MigrationOperation.values.mkString(", ")}]")
    }
  }

  SchemaMigrator(databaseTree, operation, numChanges, dryRun)
}
