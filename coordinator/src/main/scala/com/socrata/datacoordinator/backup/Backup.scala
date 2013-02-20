package com.socrata.datacoordinator.backup

import java.sql.{DriverManager, Connection}
import java.util.concurrent.{Executors, ExecutorService}

import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed

import com.socrata.soql.types.SoQLType

import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlColumnRep, DatabasePopulator}
import com.socrata.datacoordinator.truth.loader.sql._
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
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
import org.postgresql.core.BaseConnection
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class Backup(conn: Connection, executor: ExecutorService, paranoid: Boolean) {
  val typeContext = SoQLTypeContext
  val logger: Logger[Any] = NullLogger
  val datasetMap: BackupDatasetMap = new PostgresDatasetMapWriter(conn)

  def genericRepFor(columnInfo: ColumnInfo): SqlColumnRep[SoQLType, Any] =
    SoQLRep.repFactories(typeContext.typeFromName(columnInfo.typeName))(columnInfo.physicalColumnBase)

  def extractCopier(conn: Connection) = conn.asInstanceOf[BaseConnection].getCopyAPI

  val schemaLoader: SchemaLoader = new RepBasedSqlSchemaLoader(conn, logger, genericRepFor)
  val contentsCopier: DatasetContentsCopier = new RepBasedSqlDatasetContentsCopier(conn, logger, genericRepFor)
  def decsvifier(copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo]): DatasetDecsvifier =
    new PostgresDatasetDecsvifier(conn, extractCopier, copyInfo.dataTableName, schema.mapValuesStrict(genericRepFor))

  def dataLoader(version: CopyInfo): Managed[PrevettedLoader[Any]] = {
    val schemaInfo = datasetMap.schema(version)
    val schema = schemaInfo.mapValuesStrict(genericRepFor)
    val idCol = schemaInfo.values.find(_.isSystemPrimaryKey).getOrElse(sys.error("No system ID column?")).systemId
    val systemIds = schemaInfo.filter { (_, ci) => ci.logicalName.startsWith(":") }.keySet
    val datasetContext = RepBasedSqlDatasetContext(
      typeContext,
      schema,
      schemaInfo.values.find(_.isUserPrimaryKey).map(_.systemId),
      idCol,
      systemIds)
    val sqlizer = new PostgresRepBasedDataSqlizer(version.dataTableName, datasetContext, executor, extractCopier)
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

  def updateVersion(version: CopyInfo, newVersion: Long): CopyInfo =
    datasetMap.updateDataVersion(version, newVersion)

  def createDataset(prototypeDatasetInfo: UnanchoredDatasetInfo, prototypeVersionInfo: UnanchoredCopyInfo): CopyInfo = {
    require(prototypeVersionInfo.lifecycleStage == LifecycleStage.Unpublished, "Bad lifecycle stage")
    require(prototypeVersionInfo.copyNumber == 1, "Bad lifecycle version")

    val vi = datasetMap.createWithId(prototypeDatasetInfo.systemId, prototypeDatasetInfo.datasetName, prototypeDatasetInfo.tableBaseBase, prototypeVersionInfo.systemId)
    assert(vi.datasetInfo.unanchored == prototypeDatasetInfo)
    assert(vi.unanchored == prototypeVersionInfo)
    schemaLoader.create(vi)
    vi
  }

  def addColumn(currentVersion: CopyInfo, columnInfo: UnanchoredColumnInfo): currentVersion.type = {
    // TODO: Re-paranoid this
    // Resync.unless(currentVersion, currentVersion == columnInfo.copyInfo, "Copy infos differ")
    val ci = datasetMap.addColumnWithId(columnInfo.systemId, currentVersion, columnInfo.logicalName, columnInfo.typeName, columnInfo.physicalColumnBaseBase)
    schemaLoader.addColumn(ci)
    Resync.unless(ci, ci.unanchored == columnInfo, "Newly created column info differs")
    currentVersion
  }

  def dropColumn(currentVersion: CopyInfo, columnInfo: UnanchoredColumnInfo): currentVersion.type = {
    // TODO: Re-paranoid this
    // Resync.unless(currentVersion, currentVersion == columnInfo.copyInfo, "Version infos differ")
    val ci = datasetMap.schema(currentVersion).getOrElse(columnInfo.systemId, Resync(currentVersion, "No column with ID " + columnInfo.systemId))
    Resync.unless(ci, ci.unanchored == columnInfo, "Column infos differ")
    datasetMap.dropColumn(ci)
    schemaLoader.dropColumn(ci)
    currentVersion
  }

  def makePrimaryKey(currentVersionInfo: CopyInfo, columnInfo: UnanchoredColumnInfo): currentVersionInfo.type = {
    // TODO: Re-paranoid this
    // Resync.unless(currentVersionInfo, currentVersionInfo == columnInfo.copyInfo, "Version infos differ")
    val ci = datasetMap.schema(currentVersionInfo)(columnInfo.systemId)
    val ci2 = datasetMap.setUserPrimaryKey(ci)
    Resync.unless(ci2, ci2.unanchored == columnInfo, "Column infos differ:\n" + ci2.unanchored + "\n" + columnInfo)
    schemaLoader.makePrimaryKey(ci2)
    currentVersionInfo
  }

  def dropPrimaryKey(versionInfo: CopyInfo, columnInfo: UnanchoredColumnInfo): versionInfo.type = {
    val schema = datasetMap.schema(versionInfo)
    val ci = schema.values.find(_.isUserPrimaryKey).getOrElse {
      Resync(versionInfo, sys.error("No primary key on dataset"))
    }
    Resync.unless(ci, ci.unanchored == columnInfo, "Column infos differ: " + ci.unanchored + "\n" + columnInfo)
    datasetMap.clearUserPrimaryKey(ci)
    schemaLoader.dropPrimaryKey(ci)
    versionInfo
  }

  def makeSystemPrimaryKey(currentVersionInfo: CopyInfo, columnInfo: UnanchoredColumnInfo): currentVersionInfo.type = {
    // TODO: Re-paranoid this
    // Resync.unless(currentVersionInfo, currentVersionInfo == columnInfo.copyInfo, "Version infos differ")
    val ci = datasetMap.schema(currentVersionInfo)(columnInfo.systemId)
    val ci2 = datasetMap.setSystemPrimaryKey(ci)
    Resync.unless(ci2, ci2.unanchored == columnInfo, "Column infos differ:\n" + ci2.unanchored + "\n" + columnInfo)
    schemaLoader.makeSystemPrimaryKey(ci2)
    currentVersionInfo
  }

  def makeWorkingCopy(currentVersionInfo: CopyInfo, newVersionInfo: UnanchoredCopyInfo): CopyInfo = {
    val CopyPair(_, newVi) = datasetMap.createUnpublishedCopyWithId(currentVersionInfo.datasetInfo, newVersionInfo.systemId)
    Resync.unless(currentVersionInfo, newVi.unanchored == newVersionInfo, "New version info differs")
    schemaLoader.create(newVi)
    newVi
  }

  def populateWorkingCopy(currentVersion: CopyInfo): currentVersion.type = {
    Resync.unless(currentVersion, currentVersion.lifecycleStage == LifecycleStage.Unpublished, "Latest version is not unpublished")
    val oldVersion = datasetMap.published(currentVersion.datasetInfo).getOrElse {
      Resync(currentVersion, "No published copy")
    }

    val newSchema = datasetMap.schema(currentVersion)
    if(paranoid) {
      val oldSchema = datasetMap.schema(oldVersion)
      Resync.unless(currentVersion,
        oldSchema.mapValuesStrict(_.unanchored.copy(isSystemPrimaryKey = false, isUserPrimaryKey = false)) == newSchema.mapValuesStrict(_.unanchored),
        "published and unpublished schemas differ:\n" + oldSchema.mapValuesStrict(_.unanchored) + "\n" + newSchema.mapValuesStrict(_.unanchored))
    }

    contentsCopier.copy(oldVersion, currentVersion, newSchema)

    currentVersion
  }

  def dropCopy(currentVersion: CopyInfo, droppedCopy: UnanchoredCopyInfo): CopyInfo = {
    if(currentVersion.systemId == droppedCopy.systemId) {
      Resync.unless(currentVersion, currentVersion.unanchored == droppedCopy, "Told to drop the current copy, but it doesn't match")
      datasetMap.dropCopy(currentVersion)
      schemaLoader.drop(currentVersion)
      datasetMap.latest(currentVersion.datasetInfo)
    } else {
      datasetMap.copyNumber(currentVersion.datasetInfo, droppedCopy.copyNumber) match {
        case Some(toDrop) =>
          datasetMap.dropCopy(toDrop)
          schemaLoader.drop(toDrop)
          currentVersion
        case None =>
          Resync(currentVersion.datasetInfo, "Told to drop a copy that didn't exist")
      }
    }
  }

  def truncate(currentVersion: CopyInfo): currentVersion.type = {
    truncate(currentVersion.dataTableName)
    currentVersion
  }

  def populateData(currentVersionInfo: CopyInfo, ops: Seq[Operation[Any]]): currentVersionInfo.type = {
    for(loader <- dataLoader(currentVersionInfo)) {
      ops.foreach {
        case Insert(sid, row) => loader.insert(sid, row)
        case Update(sid, row) => loader.update(sid, row)
        case Delete(sid) => loader.delete(sid)
      }
    }
    currentVersionInfo
  }

  def rowIdCounterUpdated(currentVersionInfo: CopyInfo, rid: RowId): CopyInfo = {
    logger.rowIdCounterUpdated(rid)
    datasetMap.updateNextRowId(currentVersionInfo, rid)
  }

  def publish(currentVersionInfo: CopyInfo): CopyInfo =
    try {
      datasetMap.publish(currentVersionInfo)
    } catch {
      case _: IllegalArgumentException =>
        Resync(currentVersionInfo, "Not a published copy")
    }

  def dispatch(versionInfo: CopyInfo, event: Delogger.LogEvent[Any]): CopyInfo = {
    event match {
      case WorkingCopyCreated(_, newVersionInfo) =>
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
      case CopyDropped(info) =>
        dropCopy(versionInfo, info)
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

  def continuePlayback(backup: Backup)(initialVersionInfo: CopyInfo)(it: Iterator[Delogger.LogEvent[Any]]) =
    it.foldLeft(initialVersionInfo)(backup.dispatch)

  def playbackNew(primaryConn: Connection, backup: Backup, datasetSystemId: DatasetId) = {
    val delogger: Delogger[Any] = new SqlDelogger(primaryConn, "t_" + datasetSystemId.underlying + "_log", () => SoQLRowLogCodec)
    using(delogger.delog(1L)) { it =>
      it.next() match {
        case WorkingCopyCreated(newDatasetInfo, newVersionInfo) =>
          val initialVersionInfo = backup.createDataset(newDatasetInfo, newVersionInfo)
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
