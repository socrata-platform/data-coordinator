package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.sql.{DatabasePopulator, SqlPKableColumnRep, SqlColumnRep}
import com.socrata.datacoordinator.truth.metadata.DatasetMapWriter

abstract class RepBasedSchemaLoader[CT, CV](conn: Connection, logger: Logger[CV]) extends SchemaLoader {
  def repFor(columnInfo: DatasetMapWriter#ColumnInfo): SqlColumnRep[CT, CV]

  // overriddeden when things need to go in tablespaces
  def postgresTablespaceSuffix: String = ""

  def create(versionInfo: DatasetMapWriter#VersionInfo) {
    using(conn.createStatement()) { stmt =>
      stmt.execute("CREATE TABLE " + versionInfo.dataTableName + " ()" + postgresTablespaceSuffix)
      stmt.execute(DatabasePopulator.logTableCreate(versionInfo.datasetInfo.logTableName, SqlLogger.opLength))
    }
    logger.workingCopyCreated()
  }

  def addColumn(columnInfo: DatasetMapWriter#ColumnInfo) {
    val rep = repFor(columnInfo)
    using(conn.createStatement()) { stmt =>
      for((col, colTyp) <- rep.physColumns.zip(rep.sqlTypes)) {
        stmt.execute("ALTER TABLE " + columnInfo.versionInfo.dataTableName + " ADD COLUMN " + col + " " + colTyp + " NULL")
      }
    }
    logger.columnCreated(columnInfo)
  }

  def dropColumn(columnInfo: DatasetMapWriter#ColumnInfo) {
    val rep = repFor(columnInfo)
    using(conn.createStatement()) { stmt =>
      for(col <- rep.physColumns) {
        stmt.execute("ALTER TABLE " + columnInfo.versionInfo.dataTableName + " DROP COLUMN " + col)
      }
    }
    logger.columnRemoved(columnInfo)
  }

  def makePrimaryKey(columnInfo: DatasetMapWriter#ColumnInfo): Boolean = {
    repFor(columnInfo) match {
      case rep: SqlPKableColumnRep[CT, CV] =>
        using(conn.createStatement()) { stmt =>
          val table = columnInfo.versionInfo.dataTableName
          stmt.execute("CREATE INDEX " + table + "_" + rep.base + " ON " + table + "(" + rep.equalityIndexExpression + ")" + postgresTablespaceSuffix)
          for(col <- rep.physColumns) {
            stmt.execute("ALTER TABLE " + table + " ALTER " + col + " SET NOT NULL")
          }
        }
        logger.rowIdentifierChanged(Some(columnInfo))
        true
      case _ =>
        false
    }
  }

  def dropPrimaryKey(columnInfo: DatasetMapWriter#ColumnInfo): Boolean = {
    repFor(columnInfo) match {
      case rep: SqlPKableColumnRep[CT, CV] =>
        using(conn.createStatement()) { stmt =>
          val table = columnInfo.versionInfo.dataTableName
          stmt.execute("DROP INDEX " + table + "_" + rep.base)
          for(col <- rep.physColumns) {
            stmt.execute("ALTER TABLE " + table + " ALTER " + col + " DROP NOT NULL")
          }
        }
        logger.rowIdentifierChanged(None)
        true
      case _ =>
        false
    }
  }
}
