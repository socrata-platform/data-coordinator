package com.socrata.datacoordinator.backup

import java.sql.{DriverManager, Connection}

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.loader.{DatasetContentsCopier, SchemaLoader, Logger, NullLogger}
import com.socrata.datacoordinator.manifest.TruthManifest
import com.socrata.datacoordinator.manifest.sql.SqlTruthManifest
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.id.{VersionId, ColumnId, DatasetId}
import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, DatabasePopulator}
import com.socrata.datacoordinator.truth.loader.sql.{RepBasedSqlDatasetContentsCopier, RepBasedSqlSchemaLoader}
import com.socrata.soql.types.SoQLType
import com.socrata.datacoordinator.common.soql.{SystemColumns, SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMap

class Backup(conn: Connection, systemIdColumnName: String, paranoid: Boolean) {
  val typeContext = SoQLTypeContext
  val logger: Logger[Any] = NullLogger
  val datasetMap: BackupDatasetMap = new PostgresDatasetMap(conn)
  val truthManifest: TruthManifest = new SqlTruthManifest(conn)

  def genericRepFor(columnInfo: ColumnInfo): SqlColumnRep[SoQLType, Any] =
    SoQLRep.repFactories(typeContext.typeFromName(columnInfo.typeName))(columnInfo.physicalColumnBase)

  val schemaLoader: SchemaLoader = new RepBasedSqlSchemaLoader(conn, logger, genericRepFor)
  val contentsCopier: DatasetContentsCopier = new RepBasedSqlDatasetContentsCopier(conn, logger, genericRepFor)

  def resync(datasetSystemId: DatasetId, explanation: String): Nothing =
    throw new Exception(explanation)

  @inline final def resyncUnless(datasetSystemId: DatasetId, condition: Boolean, explanation: => String) {
    assert(condition, explanation)
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

  def createDataset(versionInfo: VersionInfo): VersionInfo = {
    require(versionInfo.lifecycleStage == LifecycleStage.Unpublished, "Bad lifecycle stage")
    require(versionInfo.lifecycleVersion == 1, "Bad lifecycle version")

    val vi = datasetMap.createWithId(versionInfo.datasetInfo.systemId, versionInfo.datasetInfo.datasetId, versionInfo.datasetInfo.tableBase, versionInfo.systemId)
    assert(vi == versionInfo)
    schemaLoader.create(vi)
    vi
  }

  def requireDataset(datasetInfo: DatasetInfo): datasetMap.DatasetInfo = {
    val di = datasetMap.datasetInfo(datasetInfo.systemId).getOrElse {
      resync(datasetInfo.systemId, "No dataset found")
    }
    resyncUnless(datasetInfo.systemId, di == datasetInfo, "Dataset info mismatch")
    di
  }

  def addColumn(columnInfo: ColumnInfo): ColumnInfo = {
    val di = requireDataset(columnInfo.versionInfo.datasetInfo)
    val vi = datasetMap.latest(di)
    resyncUnless(di.systemId, vi == columnInfo.versionInfo, "Version infos differ")
    val ci = datasetMap.addColumnWithId(columnInfo.systemId, vi, columnInfo.logicalName, columnInfo.typeName, columnInfo.physicalColumnBaseBase)
    resyncUnless(di.systemId, ci == columnInfo, "Newly created column info differs")
    schemaLoader.addColumn(ci)

    if(ci.logicalName == systemIdColumnName) schemaLoader.makeSystemPrimaryKey(ci)

    ci
  }

  def dropColumn(columnInfo: ColumnInfo) {
    val di = requireDataset(columnInfo.versionInfo.datasetInfo)
    val vi = datasetMap.latest(di)
    resyncUnless(di.systemId, vi == columnInfo.versionInfo, "Version infos differ")
    val ci = datasetMap.schema(vi)(columnInfo.systemId)
    assert(ci == columnInfo)
    datasetMap.dropColumn(ci)
    schemaLoader.dropColumn(ci)
  }

  def makePrimaryKey(columnInfo: ColumnInfo) {
    val di = requireDataset(columnInfo.versionInfo.datasetInfo)
    val vi = datasetMap.latest(di)
    resyncUnless(di.systemId, vi == columnInfo.versionInfo, "Version infos differ")
    val ci = datasetMap.schema(vi)(columnInfo.systemId)
    resyncUnless(di.systemId, ci == columnInfo, "Column infos differ")
    datasetMap.setUserPrimaryKey(ci)
    schemaLoader.makePrimaryKey(ci)
  }

  def dropPrimaryKey(columnInfo: ColumnInfo) {
    val di = requireDataset(columnInfo.versionInfo.datasetInfo)
    val vi = datasetMap.latest(di)
    resyncUnless(di.systemId, vi == columnInfo.versionInfo, "Version infos differ")
    val ci = datasetMap.schema(vi)(columnInfo.systemId)
    resyncUnless(di.systemId, ci == columnInfo, "Column infos differ")
    datasetMap.clearUserPrimaryKey(vi)
    schemaLoader.dropPrimaryKey(ci)
  }

  def makeWorkingCopy(newVersionInfo: VersionInfo) {
    val di = requireDataset(newVersionInfo.datasetInfo)
    val CopyPair(_, newVi) = datasetMap.createUnpublishedCopyWithId(di, newVersionInfo.systemId)
    resyncUnless(di.systemId, newVi == newVersionInfo, "New version info differs")
    schemaLoader.create(newVi)
  }

  def populateWorkingCopy(datasetInfo: DatasetInfo) {
    val di = requireDataset(datasetInfo)
    val newVi = datasetMap.latest(di)
    resyncUnless(di.systemId, newVi.lifecycleStage == LifecycleStage.Unpublished, "Latest version is not unpublished")
    val oldVi = datasetMap.published(di).getOrElse {
      resync(di.systemId, "No published copy")
    }

    val newSchema = datasetMap.schema(newVi)
    if(paranoid) {
      val oldSchema = datasetMap.schema(oldVi)
      resyncUnless(di.systemId, oldSchema == newSchema, "published and unpublished schemas differ")
    }

    contentsCopier.copy(oldVi, newVi, newSchema)
  }

  def truncate(versionInfo: VersionInfo) {
    val di = requireDataset(versionInfo.datasetInfo)
    val vi = datasetMap.latest(di)
    resyncUnless(di.systemId, vi == versionInfo, "Version infos differ")
    truncate(vi.dataTableName)
  }
}

object Backup extends App {
  using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/robertm", "blist", "blist")) { conn =>
    conn.setAutoCommit(false)
    try {
      val bkp = new Backup(conn, SystemColumns.id, paranoid = true)
      import bkp._

      DatabasePopulator.populate(conn)

      val vi = datasetMap.createWithId(new DatasetId(11001001), "one", "base", new VersionId(5))
      datasetMap.addColumnWithId(new ColumnId(50), vi, "gnu", "gnu", "gnu")
      datasetMap.addColumnWithId(new ColumnId(51), vi, "gnat", "gnu", "gnu")
      println(datasetMap.schema(vi))
    } finally {
      conn.rollback()
    }
  }
}
