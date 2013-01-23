package com.socrata.datacoordinator.backup

import java.sql.{DriverManager, Connection}

import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.manifest.TruthManifest
import com.socrata.datacoordinator.manifest.sql.SqlTruthManifest
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, DatabasePopulator}
import com.socrata.datacoordinator.truth.loader.sql._
import com.socrata.soql.types.SoQLType
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SystemColumns, SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMap
import java.util.concurrent.{Executors, ExecutorService}
import com.socrata.datacoordinator.common.sql.RepBasedDatasetContext
import com.socrata.datacoordinator.truth.loader.Delogger._
import com.socrata.datacoordinator.truth.loader.Delogger.WorkingCopyCreated
import com.socrata.datacoordinator.truth.loader.Delogger.ColumnCreated
import com.socrata.datacoordinator.truth.loader.Delogger.RowDataUpdated
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.truth.loader.Delogger.RowIdentifierChanged

class Backup(conn: Connection, systemIdColumnName: String, executor: ExecutorService, paranoid: Boolean) {
  val typeContext = SoQLTypeContext
  val logger: Logger[Any] = NullLogger
  val datasetMap: BackupDatasetMap = new PostgresDatasetMap(conn)
  val truthManifest: TruthManifest = new SqlTruthManifest(conn)

  def genericRepFor(columnInfo: ColumnInfo): SqlColumnRep[SoQLType, Any] =
    SoQLRep.repFactories(typeContext.typeFromName(columnInfo.typeName))(columnInfo.physicalColumnBase)

  val schemaLoader: SchemaLoader = new RepBasedSqlSchemaLoader(conn, logger, genericRepFor)
  val contentsCopier: DatasetContentsCopier = new RepBasedSqlDatasetContentsCopier(conn, logger, genericRepFor)

  def dataLoader(version: datasetMap.VersionInfo): Managed[PrevettedLoader[Any]] = {
    val schemaInfo = datasetMap.schema(version)
    val schema = schemaInfo.mapValuesStrict(genericRepFor)
    val idCol = schemaInfo.values.find(_.logicalName == systemIdColumnName).getOrElse(sys.error("No system ID column?")).systemId
    val systemIds = schemaInfo.filter { (_, ci) => ci.logicalName.startsWith(":") }.keySet
    val datasetContext = new RepBasedDatasetContext(
      typeContext,
      schema,
      schemaInfo.values.find(_.isUserPrimaryKey).map(_.systemId),
      idCol,
      systemIds)
    val sqlizer = new PostgresRepBasedDataSqlizer(version.dataTableName, datasetContext, executor)
    managed(new SqlPrevettedLoader(conn, sqlizer, logger))
  }

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

    val vi = datasetMap.createWithId(versionInfo.datasetInfo.systemId, versionInfo.datasetInfo.datasetId, versionInfo.datasetInfo.tableBaseBase, versionInfo.systemId)
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

  def makeSystemPrimaryKey(columnInfo: ColumnInfo) {
    val di = requireDataset(columnInfo.versionInfo.datasetInfo)
    val vi = datasetMap.latest(di)
    resyncUnless(di.systemId, vi == columnInfo.versionInfo, "Version infos differ")
    val ci = datasetMap.schema(vi)(columnInfo.systemId)
    resyncUnless(di.systemId, ci == columnInfo, "Column infos differ")
    schemaLoader.makeSystemPrimaryKey(ci)
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

  def populateData(datasetInfo: DatasetInfo, ops: Seq[Operation[Any]]) {
    val di = requireDataset(datasetInfo)
    val vi = datasetMap.latest(di)
    for(loader <- dataLoader(vi)) {
      for(op <- ops) {
        op match {
          case Insert(sid, row) => loader.insert(sid, row)
          case Update(sid, row) => loader.update(sid, row)
          case Delete(sid) => loader.delete(sid)
        }
      }
    }
  }
}

object Backup extends App {
  def playback(primaryConn: Connection, backupConn: Connection, backup: Backup, datasetSystemId: DatasetId, version: Long) {
    val delogger: Delogger[Any] = new SqlDelogger(primaryConn, "t_" + datasetSystemId.underlying + "_log", () => SoQLRowLogCodec)
    for {
      it <- managed(delogger.delog(version))
      event <- it
    } {
      event match {
        case WorkingCopyCreated(versionInfo) =>
          if(version == 1) backup.createDataset(versionInfo)
          else backup.makeWorkingCopy(versionInfo)
        case ColumnCreated(info) =>
          backup.addColumn(info)
        case RowIdentifierChanged(Some(info)) =>
          backup.makePrimaryKey(info)
        case SystemRowIdentifierChanged(info) =>
          backup.makeSystemPrimaryKey(info)
        case RowIdentifierChanged(None) =>
          val di = backup.datasetMap.datasetInfo(datasetSystemId).getOrElse(sys.error("No such dataset " + datasetSystemId))
          val vi = backup.datasetMap.latest(di)
          val schema = backup.datasetMap.schema(vi)
          val ci = schema.values.find(_.isUserPrimaryKey).getOrElse(sys.error("No primary key on dataset " + datasetSystemId))
          backup.dropPrimaryKey(ci)
        case RowDataUpdated(ops) =>
          val di = backup.datasetMap.datasetInfo(datasetSystemId).getOrElse(sys.error("No such dataset " + datasetSystemId))
          backup.populateData(di, ops)
      }
    }
  }

  val executor = Executors.newCachedThreadPool()
  try {
    for {
      primaryConn <- managed(DriverManager.getConnection("jdbc:postgresql://localhost:5432/robertm", "blist", "blist"))
      backupConn <- managed(DriverManager.getConnection("jdbc:postgresql://localhost:5432/robertm2", "blist", "blist"))
    } {
      primaryConn.setAutoCommit(false)
      backupConn.setAutoCommit(false)
      try {
        DatabasePopulator.populate(backupConn)

        val bkp = new Backup(backupConn, SystemColumns.id, executor, paranoid = true)

        for {
          globalLogStmt <- managed(primaryConn.prepareStatement("SELECT id, dataset_system_id, version FROM global_log ORDER BY id"))
          globalLogRS <- managed(globalLogStmt.executeQuery())
        } {
          while(globalLogRS.next()) {
            val datasetSystemId = new DatasetId(globalLogRS.getLong("dataset_system_id"))
            val version = globalLogRS.getLong("version")
            playback(primaryConn, backupConn, bkp, datasetSystemId, version)
          }
        }
        backupConn.commit()
        primaryConn.commit()
      } finally {
        backupConn.rollback()
        primaryConn.rollback()
      }
    }
  } finally {
    executor.shutdown()
  }
}
