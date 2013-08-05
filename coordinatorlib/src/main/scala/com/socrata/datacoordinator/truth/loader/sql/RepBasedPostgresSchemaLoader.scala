package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.sql.{DatabasePopulator, SqlPKableColumnRep, SqlColumnRep}
import com.socrata.datacoordinator.truth.metadata.{LifecycleStage, CopyInfo, ColumnInfo}

class RepBasedPostgresSchemaLoader[CT, CV](conn: Connection, logger: Logger[CT, CV], repFor: ColumnInfo[CT] => SqlColumnRep[CT, CV], tablespace: String => Option[String]) extends SchemaLoader[CT] {
  private val uniqueViolation = "23505"
  private val notNullViolation = "23502"

  private def postgresTablespaceSuffixFor(s: String): String =
    tablespace(s) match {
      case Some(ts) =>
        " TABLESPACE " + ts
      case None =>
        ""
    }

  def create(copyInfo: CopyInfo) {
    using(conn.createStatement()) { stmt =>
      stmt.execute("CREATE TABLE " + copyInfo.dataTableName + " ()" + postgresTablespaceSuffixFor(copyInfo.dataTableName))
      stmt.execute(DatabasePopulator.logTableCreate(copyInfo.datasetInfo.logTableName, SqlLogger.maxOpLength))
    }
    logger.workingCopyCreated(copyInfo)
  }

  def drop(copyInfo: CopyInfo) {
    using(conn.prepareStatement("INSERT INTO pending_table_drops (table_name, queued_at) VALUES (?, current_timestamp)")) { stmt =>
      stmt.setString(1, copyInfo.dataTableName)
      stmt.execute()
    }
    if(copyInfo.lifecycleStage == LifecycleStage.Snapshotted)
      logger.snapshotDropped(copyInfo)
    else if(copyInfo.lifecycleStage == LifecycleStage.Unpublished)
      logger.workingCopyDropped()
    else
      sys.error("Dropped a non-snapshot/working copy")
  }

  def addColumn(columnInfo: ColumnInfo[CT]) {
    val rep = repFor(columnInfo)
    using(conn.createStatement()) { stmt =>
      for((col, colTyp) <- rep.physColumns.zip(rep.sqlTypes)) {
        stmt.execute("ALTER TABLE " + columnInfo.copyInfo.dataTableName + " ADD COLUMN " + col + " " + colTyp + " NULL")
      }
    }
    logger.columnCreated(columnInfo)
  }

  def dropColumn(columnInfo: ColumnInfo[CT]) {
    val rep = repFor(columnInfo)
    using(conn.createStatement()) { stmt =>
      for(col <- rep.physColumns) {
        stmt.execute("ALTER TABLE " + columnInfo.copyInfo.dataTableName + " DROP COLUMN " + col)
      }
    }
    logger.columnRemoved(columnInfo)
  }

  def makePrimaryKey(columnInfo: ColumnInfo[CT]) {
    makePrimaryKeyWithoutLogging(columnInfo)
    logger.rowIdentifierSet(columnInfo)
  }

  def makeSystemPrimaryKey(columnInfo: ColumnInfo[CT]) {
    makePrimaryKeyWithoutLogging(columnInfo)
    logger.systemIdColumnSet(columnInfo)
  }

  def makeVersion(columnInfo: ColumnInfo[CT]) {
    logger.versionColumnSet(columnInfo)
  }

  def makePrimaryKeyWithoutLogging(columnInfo: ColumnInfo[CT]) {
    repFor(columnInfo) match {
      case rep: SqlPKableColumnRep[CT, CV] =>
        using(conn.createStatement()) { stmt =>
          val table = columnInfo.copyInfo.dataTableName
          try {
            for(col <- rep.physColumns) {
              stmt.execute("ALTER TABLE " + table + " ALTER " + col + " SET NOT NULL")
            }
            val indexName = "uniq_" + table + "_" + rep.base
            stmt.execute("CREATE UNIQUE INDEX " + indexName + " ON " + table + "(" + rep.equalityIndexExpression + ")" + postgresTablespaceSuffixFor(indexName))
          } catch {
            case e: java.sql.SQLException if e.getSQLState == uniqueViolation =>
              throw DuplicateValuesInColumn(columnInfo.userColumnId)
            case e: java.sql.SQLException if e.getSQLState == notNullViolation =>
              throw NullValuesInColumn(columnInfo.userColumnId)
          }
        }
      case _ =>
        throw NotPKableType(columnInfo.userColumnId, columnInfo.typ)
    }
  }

  def dropPrimaryKey(columnInfo: ColumnInfo[CT]): Boolean = {
    repFor(columnInfo) match {
      case rep: SqlPKableColumnRep[CT, CV] =>
        using(conn.createStatement()) { stmt =>
          val table = columnInfo.copyInfo.dataTableName
          stmt.execute("DROP INDEX uniq_" + table + "_" + rep.base)
          for(col <- rep.physColumns) {
            stmt.execute("ALTER TABLE " + table + " ALTER " + col + " DROP NOT NULL")
          }
        }
        logger.rowIdentifierCleared(columnInfo)
        true
      case _ =>
        false
    }
  }
}
