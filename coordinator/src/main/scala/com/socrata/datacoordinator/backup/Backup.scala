package com.socrata.datacoordinator.backup

import java.sql.{DriverManager, Connection}

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.loader.{SchemaLoader, Logger, NullLogger}
import com.socrata.datacoordinator.manifest.TruthManifest
import com.socrata.datacoordinator.manifest.sql.SqlTruthManifest
import com.socrata.datacoordinator.truth.metadata.{VersionInfo, ColumnInfo, BackupDatasetMapWriter}
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
import com.socrata.datacoordinator.id.{VersionId, ColumnId, DatasetId}
import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, DatabasePopulator}
import com.socrata.datacoordinator.truth.loader.sql.RepBasedSqlSchemaLoader
import com.socrata.soql.types.SoQLType
import com.socrata.datacoordinator.common.soql.{SystemColumns, SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class Backup(conn: Connection) {
  val typeContext = SoQLTypeContext
  val logger: Logger[Any] = NullLogger
  val datasetMapWriter: BackupDatasetMapWriter = new PostgresDatasetMapWriter(conn)
  val truthManifest: TruthManifest = new SqlTruthManifest(conn)

  def genericRepFor(columnInfo: ColumnInfo): SqlColumnRep[SoQLType, Any] =
    SoQLRep.repFactories(typeContext.typeFromName(columnInfo.typeName))(columnInfo.physicalColumnBase)

  val schemaLoader: SchemaLoader = new RepBasedSqlSchemaLoader[SoQLType, Any](conn, logger, genericRepFor)

  // TODO: Move this elsewhere, add logging, etc.
  // (actually, does this need logging?  This will never get called except after an
  // "ensureWorkingCopy", and ALWAYS after that if it does create one.)
  def populate(srcTable: String, dstTable: String, schema: ColumnIdMap[ColumnInfo]) {
    if(schema.nonEmpty) { // it should never be empty, but this should be correct even if it is.
      val physCols = schema.values.flatMap(genericRepFor(_).physColumns).mkString(",")
      using(conn.createStatement()) { stmt =>
        stmt.execute(s"INSERT INTO $dstTable ($physCols) SELECT $physCols FROM $srcTable")
        // TODO: schedule dstTable for a VACUUM ANALYZE (since it can't happen in a transaction)
      }
    }
  }

  // TODO: Move this elsewhere, add logging, etc.
  def truncate(table: String) {
    using(conn.createStatement()) { stmt =>
      stmt.execute(s"DELETE FROM $table")
      // TODO: schedule table for a VACUUM ANALYZE (since it can't happen in a transaction)
    }
  }

  def updateVersion(version: VersionInfo) {
    truthManifest.updateLatestVersion(version.datasetInfo, version.lifecycleVersion)
  }

  def createDataset(versionInfo: VersionInfo): datasetMapWriter.VersionInfo = {
    val vi = datasetMapWriter.createWithId(versionInfo.datasetInfo.systemId, versionInfo.datasetInfo.datasetId, versionInfo.datasetInfo.tableBase, versionInfo.systemId)
    assert(vi == versionInfo)
    schemaLoader.create(vi)
    vi
  }

  def addColumn(columnInfo: ColumnInfo): datasetMapWriter.ColumnInfo = {
    val di = datasetMapWriter.datasetInfo(columnInfo.versionInfo.datasetInfo.systemId).getOrElse {
      throw new Exception("Cannot find dataset " + columnInfo.versionInfo.datasetInfo.systemId.underlying) // TODO: real exception
    }
    val vi = datasetMapWriter.latest(di)
    val ci = datasetMapWriter.addColumnWithId(columnInfo.systemId, vi, columnInfo.logicalName, columnInfo.typeName, columnInfo.physicalColumnBaseBase)
    assert(ci == columnInfo)
    schemaLoader.addColumn(ci)
    ci
  }

  def dropColumn(columnInfo: ColumnInfo) {
    val di = datasetMapWriter.datasetInfo(columnInfo.versionInfo.datasetInfo.systemId).getOrElse {
      throw new Exception("Cannot find dataset " + columnInfo.versionInfo.datasetInfo.systemId.underlying) // TODO: real exception
    }
    val vi = datasetMapWriter.latest(di)
    val ci = datasetMapWriter.schema(vi)(columnInfo.systemId)
    assert(ci == columnInfo)
    datasetMapWriter.dropColumn(ci)
    schemaLoader.dropColumn(ci)
  }

  def makePrimaryKey(columnInfo: ColumnInfo) {
    val di = datasetMapWriter.datasetInfo(columnInfo.versionInfo.datasetInfo.systemId).getOrElse {
      throw new Exception("Cannot find dataset " + columnInfo.versionInfo.datasetInfo.systemId.underlying) // TODO: real exception
    }
    val vi = datasetMapWriter.latest(di)
    val ci = datasetMapWriter.schema(vi)(columnInfo.systemId)
    assert(ci == columnInfo)
    datasetMapWriter.setUserPrimaryKey(ci)
    schemaLoader.makePrimaryKey(ci)
  }

  def dropPrimaryKey(columnInfo: ColumnInfo) {
    val di = datasetMapWriter.datasetInfo(columnInfo.versionInfo.datasetInfo.systemId).getOrElse {
      throw new Exception("Cannot find dataset " + columnInfo.versionInfo.datasetInfo.systemId.underlying) // TODO: real exception
    }
    val vi = datasetMapWriter.latest(di)
    val ci = datasetMapWriter.schema(vi)(columnInfo.systemId)
    assert(ci == columnInfo)
    datasetMapWriter.clearUserPrimaryKey(vi)
    schemaLoader.dropPrimaryKey(ci)
  }

  def makeWorkingCopy(oldVersionInfo: VersionInfo, newVersionInfo: VersionInfo) {
    val di = datasetMapWriter.datasetInfo(newVersionInfo.datasetInfo.systemId).getOrElse {
      throw new Exception("Cannot find dataset " + newVersionInfo.datasetInfo.systemId.underlying) // TODO: real exception
    }
    val vi = datasetMapWriter.createUnpublishedCopyWithId(di, newVersionInfo.systemId)
    assert(vi.newVersionInfo == newVersionInfo)
    assert(vi.oldVersionInfo == oldVersionInfo, "Didn't create a working copy of the same version as the original")
    schemaLoader.create(vi.newVersionInfo)
    val schema = datasetMapWriter.schema(vi.newVersionInfo)

    for(ci <- schema.values) {
      schemaLoader.addColumn(ci)
      if(ci.logicalName == SystemColumns.id) schemaLoader.makeSystemPrimaryKey(ci)
      else if(ci.isUserPrimaryKey) schemaLoader.makePrimaryKey(ci)
    }

    populate(vi.oldVersionInfo.dataTableName, vi.newVersionInfo.dataTableName, schema)
  }

  def truncate(versionInfo: VersionInfo) {
    val di = datasetMapWriter.datasetInfo(versionInfo.datasetInfo.systemId).getOrElse {
      throw new Exception("Cannot find dataset " + versionInfo.datasetInfo.systemId.underlying) // TODO: real exception
    }
    val vi = datasetMapWriter.latest(di)
    assert(vi == versionInfo)
    truncate(vi.dataTableName)
  }
}

object Backup extends App {
  using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/robertm", "blist", "blist")) { conn =>
    conn.setAutoCommit(false)
    try {
      val bkp = new Backup(conn)
      import bkp._

      DatabasePopulator.populate(conn)

      val vi = datasetMapWriter.createWithId(new DatasetId(11001001), "one", "base", new VersionId(5))
      datasetMapWriter.addColumnWithId(new ColumnId(50), vi, "gnu", "gnu", "gnu")
      datasetMapWriter.addColumnWithId(new ColumnId(51), vi, "gnat", "gnu", "gnu")
      println(datasetMapWriter.schema(vi))
    } finally {
      conn.rollback()
    }
  }
}
