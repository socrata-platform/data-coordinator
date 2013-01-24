package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.sql.{DatabasePopulator, SqlPKableColumnRep, SqlColumnRep}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}

class RepBasedSqlSchemaLoader[CT, CV](conn: Connection, logger: Logger[CV], repFor: ColumnInfo => SqlColumnRep[CT, CV]) extends SchemaLoader {
  // overriddeden when things need to go in tablespaces
  def postgresTablespaceSuffix: String = ""

  def create(copyInfo: CopyInfo) {
    using(conn.createStatement()) { stmt =>
      stmt.execute("CREATE TABLE " + copyInfo.dataTableName + " ()" + postgresTablespaceSuffix)
      stmt.execute(DatabasePopulator.logTableCreate(copyInfo.datasetInfo.logTableName, SqlLogger.opLength))
    }
    logger.workingCopyCreated(copyInfo)
  }

  def addColumn(columnInfo: ColumnInfo) {
    val rep = repFor(columnInfo)
    using(conn.createStatement()) { stmt =>
      for((col, colTyp) <- rep.physColumns.zip(rep.sqlTypes)) {
        stmt.execute("ALTER TABLE " + columnInfo.copyInfo.dataTableName + " ADD COLUMN " + col + " " + colTyp + " NULL")
      }
    }
    logger.columnCreated(columnInfo)
  }

  def dropColumn(columnInfo: ColumnInfo) {
    val rep = repFor(columnInfo)
    using(conn.createStatement()) { stmt =>
      for(col <- rep.physColumns) {
        stmt.execute("ALTER TABLE " + columnInfo.copyInfo.dataTableName + " DROP COLUMN " + col)
      }
    }
    logger.columnRemoved(columnInfo)
  }

  def makePrimaryKey(columnInfo: ColumnInfo): Boolean = {
    if(makePrimaryKeyWithoutLogging(columnInfo)) {
      logger.rowIdentifierSet(columnInfo)
      true
    } else {
      false
    }
  }

  def makeSystemPrimaryKey(columnInfo: ColumnInfo): Boolean =
    if(makePrimaryKeyWithoutLogging(columnInfo)) {
      logger.systemIdColumnSet(columnInfo)
      true
    } else {
      false
    }

  def makePrimaryKeyWithoutLogging(columnInfo: ColumnInfo): Boolean = {
    repFor(columnInfo) match {
      case rep: SqlPKableColumnRep[CT, CV] =>
        using(conn.createStatement()) { stmt =>
          val table = columnInfo.copyInfo.dataTableName
          for(col <- rep.physColumns) {
            stmt.execute("ALTER TABLE " + table + " ALTER " + col + " SET NOT NULL")
          }
          stmt.execute("CREATE UNIQUE INDEX uniq_" + table + "_" + rep.base + " ON " + table + "(" + rep.equalityIndexExpression + ")" + postgresTablespaceSuffix)
        }
        true
      case _ =>
        false
    }
  }

  def dropPrimaryKey(columnInfo: ColumnInfo): Boolean = {
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
