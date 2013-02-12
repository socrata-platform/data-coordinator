package com.socrata.datacoordinator.backup

import java.sql.{DriverManager, Connection}
import java.util.concurrent.{Executors, ExecutorService}

import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed

import com.socrata.soql.types.SoQLType

import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, DatabasePopulator}
import com.socrata.datacoordinator.truth.loader.sql._
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
import com.socrata.datacoordinator.common.sql.RepBasedDatasetContext
import com.socrata.datacoordinator.truth.loader.Delogger._
import com.socrata.datacoordinator.truth.loader.Delogger.RowIdentifierCleared
import com.socrata.datacoordinator.truth.loader.Update
import com.socrata.datacoordinator.truth.loader.Delogger.RowIdCounterUpdated
import com.socrata.datacoordinator.truth.loader.Delogger.RowDataUpdated
import com.socrata.datacoordinator.truth.loader.Delogger.Truncated
import com.socrata.datacoordinator.truth.loader.Delogger.WorkingCopyCreated
import com.socrata.datacoordinator.truth.loader.Delogger.RowIdentifierSet
import com.socrata.datacoordinator.truth.loader.Delogger.ColumnCreated
import com.socrata.datacoordinator.truth.loader.Delogger.SystemRowIdentifierChanged
import com.socrata.datacoordinator.truth.loader.Delete
import com.socrata.datacoordinator.truth.loader.Delogger.ColumnRemoved
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.truth.loader.Insert
import com.socrata.datacoordinator.common.StandardDatasetMapLimits

class Backup(conn: Connection, executor: ExecutorService, paranoid: Boolean) {
  val typeContext = SoQLTypeContext
  val logger: Logger[Any] = NullLogger
  val datasetMap: BackupDatasetMap = new PostgresDatasetMapWriter(conn)

  def genericRepFor(columnInfo: ColumnInfo): SqlColumnRep[SoQLType, Any] =
    SoQLRep.repFactories(typeContext.typeFromName(columnInfo.typeName))(columnInfo.physicalColumnBase)

  val schemaLoader: SchemaLoader = new RepBasedSqlSchemaLoader(conn, logger, genericRepFor)
  val contentsCopier: DatasetContentsCopier = new RepBasedSqlDatasetContentsCopier(conn, logger, genericRepFor)

  def dataLoader(version: datasetMap.CopyInfo): Managed[PrevettedLoader[Any]] = {
    val schemaInfo = datasetMap.schema(version)
    val schema = schemaInfo.mapValuesStrict(genericRepFor)
    val idCol = schemaInfo.values.find(_.isSystemPrimaryKey).getOrElse(sys.error("No system ID column?")).systemId
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

  // TODO: Move this elsewhere, etc.
  def truncate(table: String) {
    using(conn.createStatement()) { stmt =>
      stmt.execute(s"DELETE FROM $table")
      // TODO: schedule table for a VACUUM ANALYZE (since it can't happen in a transaction)
    }
    logger.truncated()
  }

  def updateVersion(version: datasetMap.CopyInfo, newVersion: Long): datasetMap.CopyInfo =
    datasetMap.updateDataVersion(version, newVersion)

  def createDataset(versionInfo: CopyInfo): datasetMap.CopyInfo = {
    require(versionInfo.lifecycleStage == LifecycleStage.Unpublished, "Bad lifecycle stage")
    require(versionInfo.copyNumber == 1, "Bad lifecycle version")

    val vi = datasetMap.createWithId(versionInfo.datasetInfo.systemId, versionInfo.datasetInfo.datasetId, versionInfo.datasetInfo.tableBaseBase, versionInfo.systemId)
    assert(vi == versionInfo)
    schemaLoader.create(vi)
    vi
  }

  def addColumn(currentVersion: datasetMap.CopyInfo, columnInfo: ColumnInfo): currentVersion.type = {
    Resync.unless(currentVersion, currentVersion == columnInfo.copyInfo, "Copy infos differ")
    val ci = datasetMap.addColumnWithId(columnInfo.systemId, currentVersion, columnInfo.logicalName, columnInfo.typeName, columnInfo.physicalColumnBaseBase)
    schemaLoader.addColumn(ci)
    Resync.unless(ci, ci == columnInfo, "Newly created column info differs")
    currentVersion
  }

  def dropColumn(currentVersion: datasetMap.CopyInfo, columnInfo: ColumnInfo): currentVersion.type = {
    Resync.unless(currentVersion, currentVersion == columnInfo.copyInfo, "Version infos differ")
    val ci = datasetMap.schema(currentVersion).getOrElse(columnInfo.systemId, Resync(currentVersion, "No column with ID " + columnInfo.systemId))
    Resync.unless(ci, ci == columnInfo, "Column infos differ")
    datasetMap.dropColumn(ci)
    schemaLoader.dropColumn(ci)
    currentVersion
  }

  def makePrimaryKey(currentVersionInfo: datasetMap.CopyInfo, columnInfo: ColumnInfo): currentVersionInfo.type = {
    Resync.unless(currentVersionInfo, currentVersionInfo == columnInfo.copyInfo, "Version infos differ")
    val ci = datasetMap.schema(currentVersionInfo)(columnInfo.systemId)
    Resync.unless(ci, ci == columnInfo, "Column infos differ")
    datasetMap.setUserPrimaryKey(ci)
    schemaLoader.makePrimaryKey(ci)
    currentVersionInfo
  }

  def dropPrimaryKey(versionInfo: datasetMap.CopyInfo, columnInfo: ColumnInfo): versionInfo.type = {
    val schema = datasetMap.schema(versionInfo)
    val ci = schema.values.find(_.isUserPrimaryKey).getOrElse {
      Resync(versionInfo, sys.error("No primary key on dataset"))
    }
    Resync.unless(ci, ci == columnInfo, "Column infos differ")
    datasetMap.clearUserPrimaryKey(ci)
    schemaLoader.dropPrimaryKey(ci)
    versionInfo
  }

  def makeSystemPrimaryKey(currentVersionInfo: datasetMap.CopyInfo, columnInfo: ColumnInfo): currentVersionInfo.type = {
    Resync.unless(currentVersionInfo, currentVersionInfo == columnInfo.copyInfo, "Version infos differ")
    val ci = datasetMap.schema(currentVersionInfo)(columnInfo.systemId)
    Resync.unless(ci, ci == columnInfo, "Column infos differ")
    datasetMap.setSystemPrimaryKey(ci)
    schemaLoader.makeSystemPrimaryKey(ci)
    currentVersionInfo
  }

  def makeWorkingCopy(currentVersionInfo: datasetMap.CopyInfo, newVersionInfo: CopyInfo): datasetMap.CopyInfo = {
    val CopyPair(_, newVi) = datasetMap.createUnpublishedCopyWithId(currentVersionInfo.datasetInfo, newVersionInfo.systemId)
    Resync.unless(currentVersionInfo, newVi == newVersionInfo, "New version info differs")
    schemaLoader.create(newVi)
    newVi
  }

  def populateWorkingCopy(currentVersion: datasetMap.CopyInfo): currentVersion.type = {
    Resync.unless(currentVersion, currentVersion.lifecycleStage == LifecycleStage.Unpublished, "Latest version is not unpublished")
    val oldVersion = datasetMap.published(currentVersion.datasetInfo).getOrElse {
      Resync(currentVersion, "No published copy")
    }

    val newSchema = datasetMap.schema(currentVersion)
    if(paranoid) {
      val oldSchema = datasetMap.schema(oldVersion)
      Resync.unless(currentVersion,
        oldSchema.mapValuesStrict(_.withCopyInfo(null)) == newSchema.mapValuesStrict(_.withCopyInfo(null)),
        "published and unpublished schemas differ")
    }

    contentsCopier.copy(oldVersion, currentVersion, newSchema)

    currentVersion
  }

  def dropWorkingCopy(currentVersion: datasetMap.CopyInfo): datasetMap.CopyInfo = {
    schemaLoader.drop(currentVersion)
    datasetMap.dropCopy(currentVersion)
    datasetMap.latest(currentVersion.datasetInfo)
  }

  def truncate(currentVersion: datasetMap.CopyInfo): currentVersion.type = {
    truncate(currentVersion.dataTableName)
    currentVersion
  }

  def populateData(currentVersionInfo: datasetMap.CopyInfo, ops: Seq[Operation[Any]]): currentVersionInfo.type = {
    for(loader <- dataLoader(currentVersionInfo)) {
      ops.foreach {
        case Insert(sid, row) => loader.insert(sid, row)
        case Update(sid, row) => loader.update(sid, row)
        case Delete(sid) => loader.delete(sid)
      }
    }
    currentVersionInfo
  }

  def rowIdCounterUpdated(currentVersionInfo: datasetMap.CopyInfo, rid: RowId): datasetMap.CopyInfo = {
    logger.rowIdCounterUpdated(rid)
    datasetMap.updateNextRowId(currentVersionInfo, rid)
  }

  def publish(currentVersionInfo: datasetMap.CopyInfo): datasetMap.CopyInfo =
    try {
      datasetMap.publish(currentVersionInfo)
    } catch {
      case _: IllegalArgumentException =>
        Resync(currentVersionInfo, "Not a published copy")
    }

  def dispatch(versionInfo: datasetMap.CopyInfo, event: Delogger.LogEvent[Any]): datasetMap.CopyInfo = {
    event match {
      case WorkingCopyCreated(newVersionInfo) =>
        makeWorkingCopy(versionInfo, newVersionInfo)
      case ColumnCreated(info) =>
        addColumn(versionInfo, info)
      case RowIdentifierSet(info) =>
        makePrimaryKey(versionInfo, info)
      case SystemRowIdentifierChanged(info) =>
        makeSystemPrimaryKey(versionInfo, info)
      case RowIdentifierCleared(info) =>
        dropPrimaryKey(versionInfo, info)
      case r@RowDataUpdated(_) =>
        populateData(versionInfo, r.operations)
      case RowIdCounterUpdated(rid) =>
        rowIdCounterUpdated(versionInfo, rid)
      case WorkingCopyPublished =>
        publish(versionInfo)
      case DataCopied =>
        populateWorkingCopy(versionInfo)
      case ColumnRemoved(info) =>
        dropColumn(versionInfo, info)
      case Truncated =>
        truncate(versionInfo)
      case WorkingCopyDropped =>
        dropWorkingCopy(versionInfo)
      case EndTransaction =>
        sys.error("Shouldn't have seen this")
    }
  }
}

object Backup extends App {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Backup])

  def playback(primaryConn: Connection, backup: Backup, datasetSystemId: DatasetId, version: Long) {
    log.info("Playing back logs for version {}", version)

    // ok, if "version" is 1, then the dataset doesn't (or rather shouldn't) exist.  If it's not, it should.
    // If either of those assumptions seems violated, then we need to do a full resync...
    val newVersion = if(version == 1L) {
      playbackNew(primaryConn, backup, datasetSystemId)
    } else {
      playbackExisting(primaryConn, backup, datasetSystemId, version)
    }

    backup.updateVersion(newVersion, version)

    log.info("Finished playing back logs for version {}", version)
  }

  def continuePlayback(backup: Backup)(initialVersionInfo: backup.datasetMap.CopyInfo)(it: Iterator[Delogger.LogEvent[Any]]) = {
    // method with dependent type (versionInfo: backup.datasetMap.CopyInfo, event: com.socrata.datacoordinator.truth.loader.Delogger.LogEvent[Any])backup.datasetMap.CopyInfo cannot be converted to function value
    // *sigh*
    // was originally just it.foldLeft(initialVersionInfo)(backup.dispatch)
    var result = initialVersionInfo
    while(it.hasNext) {
      result = backup.dispatch(result, it.next())
    }
    result
  }

  def playbackNew(primaryConn: Connection, backup: Backup, datasetSystemId: DatasetId) = {
    val delogger: Delogger[Any] = new SqlDelogger(primaryConn, "t_" + datasetSystemId.underlying + "_log", () => SoQLRowLogCodec)
    using(delogger.delog(1L)) { it =>
      it.next() match {
        case WorkingCopyCreated(newVersionInfo) =>
          val initialVersionInfo = backup.createDataset(newVersionInfo)
          continuePlayback(backup)(initialVersionInfo)(it)
        case _ =>
          Resync(datasetSystemId, "First operation in the log was not `create the dataset'")
      }
    }
  }

  def playbackExisting(primaryConn: Connection, backup: Backup, datasetSystemId: DatasetId, version: Long) = {
    val datasetInfo = backup.datasetMap.datasetInfo(datasetSystemId).getOrElse {
      Resync(datasetSystemId, "Expected dataset " + datasetSystemId.underlying + " to exist for version " + version)
    }
    val initialVersionInfo = backup.datasetMap.latest(datasetInfo)
    Resync.unless(datasetSystemId, initialVersionInfo.dataVersion == version - 1, "Received a change to version " + version + " but this is only at " + initialVersionInfo.dataVersion)
    val delogger: Delogger[Any] = new SqlDelogger(primaryConn, datasetInfo.logTableName, () => SoQLRowLogCodec)
    using(delogger.delog(version)) { it =>
      continuePlayback(backup)(initialVersionInfo)(it)
    }
  }

  val datasetMapLimits = StandardDatasetMapLimits

  val executor = Executors.newCachedThreadPool()
  try {
    for {
      primaryConn <- managed(DriverManager.getConnection("jdbc:postgresql://localhost:5432/robertm", "blist", "blist"))
      backupConn <- managed(DriverManager.getConnection("jdbc:postgresql://localhost:5432/robertm2", "blist", "blist"))
    } {
      primaryConn.setAutoCommit(false)
      backupConn.setAutoCommit(false)
      try {
        DatabasePopulator.populate(backupConn, datasetMapLimits)

        val bkp = new Backup(backupConn, executor, paranoid = true)

        for {
          globalLogStmt <- managed(primaryConn.prepareStatement("SELECT id, dataset_system_id, version FROM global_log ORDER BY id"))
          globalLogRS <- managed(globalLogStmt.executeQuery())
        } {
          while(globalLogRS.next()) {
            val datasetSystemId = new DatasetId(globalLogRS.getLong("dataset_system_id"))
            val version = globalLogRS.getLong("version")
            playback(primaryConn, bkp, datasetSystemId, version)
            backupConn.commit()
          }
        }
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
