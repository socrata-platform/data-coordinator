package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.sql.{DatabasePopulator, SqlColumnRep, SqlPKableColumnRep}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, ComputationStrategyInfo, CopyInfo, LifecycleStage}

class RepBasedPostgresSchemaLoader[CT, CV](conn: Connection, logger: Logger[CT, CV], repFor: ColumnInfo[CT] => SqlColumnRep[CT, CV], tablespace: String => Option[String]) extends SchemaLoader[CT] {
  private val uniqueViolation = "23505"
  private val notNullViolation = "23502"

  private def postgresTablespaceSuffixFor(tablespace: Option[String]): String =
    tablespace match {
      case Some(ts) =>
        " TABLESPACE " + ts
      case None =>
        ""
    }

  // None == table did not exist; Some(None) == default tablespace; Some(Some(ts)) == specific tablespace
  def tablespaceOfTable(tableName: String): Option[Option[String]] =
    using(conn.prepareStatement("SELECT tablespace FROM pg_tables WHERE schemaname = ? AND tablename = ?")) { stmt =>
      stmt.setString(1, "public")
      stmt.setString(2, tableName)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(Option(rs.getString("tablespace")))
        } else {
          None
        }
      }
    }

  def create(copyInfo: CopyInfo) {
    // if copyInfo.auditTableName exists, we want to use its tablespace.  Othewise we'll ask
    // postgresTablespaceSuffixFor to generate one.

    val ts: Option[String] =
      tablespaceOfTable(copyInfo.datasetInfo.auditTableName).getOrElse(tablespace(copyInfo.datasetInfo.auditTableName))
    using(conn.createStatement()) { stmt =>
      stmt.execute("CREATE TABLE " + copyInfo.dataTableName + " ()" + postgresTablespaceSuffixFor(ts) + ";" +
                   ChangeOwner.sql(conn, copyInfo.dataTableName))
      stmt.execute(DatabasePopulator.logTableCreate(
        copyInfo.datasetInfo.auditTableName,
        copyInfo.datasetInfo.logTableName,
        40, // max user uid length
        SqlLogger.maxOpLength,
        ChangeOwner.canonicalUser(conn),
        ts))
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

  def addColumns(columnInfo: Iterable[ColumnInfo[CT]]) {
    if(columnInfo.isEmpty) return; // ok?
    assert(columnInfo.forall { ci => ci.copyInfo == columnInfo.head.copyInfo }, "Columns come from different copies?")
    using(conn.createStatement()) { stmt =>
      val qb = new StringBuilder("ALTER TABLE " + columnInfo.head.copyInfo.dataTableName)
      var didOne = false
      for(ci <- columnInfo) {
        val rep = repFor(ci)
        for((col, colTyp) <- rep.physColumns.zip(rep.sqlTypes)) {
          if(didOne) qb.append(',')
          else didOne = true
          qb.append(" ADD COLUMN " + col + " " + colTyp + " NULL")
        }
        logger.columnCreated(ci)
      }
      stmt.execute(qb.toString)
    }
  }

  def dropColumns(columnInfo: Iterable[ColumnInfo[CT]]) {
    if(columnInfo.isEmpty) return; // ok?
    assert(columnInfo.forall { ci => ci.copyInfo == columnInfo.head.copyInfo }, "Columns come from different copies?")
    using(conn.createStatement()) { stmt =>
      val qb = new StringBuilder("ALTER TABLE " + columnInfo.head.copyInfo.dataTableName)
      var didOne = false
      for(ci <- columnInfo) {
        val rep = repFor(ci)
        for(col <- rep.physColumns) {
          if(didOne) qb.append(',')
          else didOne = true
          qb.append(" DROP COLUMN " + col)
        }
        logger.columnRemoved(ci)
      }
      stmt.execute(qb.toString)
    }
  }

  def addComputationStrategy(columnInfo: ColumnInfo[CT], computationStrategyInfo: ComputationStrategyInfo): Unit = {
    logger.computationStrategyCreated(columnInfo, computationStrategyInfo)
  }

  def dropComputationStrategy(columnInfo: ColumnInfo[CT]): Unit = {
    logger.computationStrategyRemoved(columnInfo)
  }

  def updateFieldName(columnInfo: ColumnInfo[CT]): Unit = {
    logger.fieldNameUpdated(columnInfo)
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
        val table = columnInfo.copyInfo.dataTableName
        val ts = tablespaceOfTable(table).getOrElse {
          sys.error("Setting a primary key on table " + table + ", which does not exist?")
        }
        using(conn.createStatement()) { stmt =>
          try {
            for(col <- rep.physColumns) {
              stmt.execute("ALTER TABLE " + table + " ALTER " + col + " SET NOT NULL")
            }
            val indexName = "uniq_" + table + "_" + rep.base
            stmt.execute("CREATE UNIQUE INDEX " + indexName + " ON " + table + "(" + rep.equalityIndexExpression + ")" + postgresTablespaceSuffixFor(ts))
            stmt.execute("ANALYZE " + table + " (" + rep.physColumns.mkString(",") + ")")
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
