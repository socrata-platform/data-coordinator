package com.socrata.datacoordinator.backup

import java.sql.{DriverManager, Connection}

import com.rojoma.simplearm.util._

import com.socrata.soql.types.{SoQLValue, SoQLType}

import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.id.{UserColumnId, DatasetId}
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlColumnRep}
import com.socrata.datacoordinator.truth.loader.sql._
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
import com.socrata.datacoordinator.truth.loader.Delogger._
import com.socrata.datacoordinator.common.StandardDatasetMapLimits
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import java.io.OutputStream
import com.socrata.datacoordinator.truth.loader.Update
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.loader.Delete
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.truth.loader.Insert
import com.socrata.datacoordinator.util.{NoopTimingReport, TimingReport}
import scala.concurrent.duration.Duration
import com.socrata.soql.environment.ColumnName
import scala.collection.mutable.ListBuffer
import com.socrata.datacoordinator.Row
import org.joda.time.DateTime

class Backup(conn: Connection, /*executor: ExecutorService, */ isSystemColumnId: UserColumnId => Boolean, timingReport: TimingReport, paranoid: Boolean, copyIn: (Connection, String, OutputStream => Unit) => Long) {
  val typeContext = SoQLTypeContext
  val logger: Logger[SoQLType, SoQLValue] = NullLogger[SoQLType, SoQLValue]
  val typeNamespace = SoQLTypeContext.typeNamespace
  val datasetMap: BackupDatasetMap[SoQLType] = new PostgresDatasetMapWriter(conn, typeNamespace, timingReport, () => sys.error("Backup should never generate obfuscation keys"), 0L)
  def tablespace(s: String) = None

  def genericRepFor(columnInfo: ColumnInfo[SoQLType]): SqlColumnRep[SoQLType, SoQLValue] =
    SoQLRep.sqlRep(columnInfo)

  def extractCopier(conn: Connection, sql: String, output: OutputStream => Unit): Long = copyIn(conn, sql, output)

  val schemaLoader: SchemaLoader[SoQLType] = new RepBasedPostgresSchemaLoader(conn, logger, genericRepFor, tablespace)
  val contentsCopier: DatasetContentsCopier[SoQLType] = new RepBasedSqlDatasetContentsCopier(conn, logger, genericRepFor, timingReport)
  def decsvifier(copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]]): DatasetDecsvifier =
    new PostgresDatasetDecsvifier(conn, extractCopier, copyInfo.dataTableName, schema.mapValuesStrict(genericRepFor))

  def dataLoader(version: CopyInfo): PrevettedLoader[SoQLValue] = {
    val schemaInfo = datasetMap.schema(version)
    val schema = schemaInfo.mapValuesStrict(genericRepFor)
    val idCol = schemaInfo.values.find(_.isSystemPrimaryKey).getOrElse(sys.error("No system ID column?")).systemId
    val versionCol = schemaInfo.values.find(_.isVersion).getOrElse(sys.error("No version column?")).systemId
    val systemIds = schemaInfo.filter { (_, ci) => isSystemColumnId(ci.userColumnId) }.keySet
    val datasetContext = RepBasedSqlDatasetContext(
      typeContext,
      schema,
      schemaInfo.values.find(_.isUserPrimaryKey).map(_.systemId),
      idCol,
      versionCol,
      systemIds)
    val sqlizer = new PostgresRepBasedDataSqlizer(version.dataTableName, datasetContext, extractCopier)
    new SqlPrevettedLoader(conn, sqlizer, logger)
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

    val vi = datasetMap.createWithId(prototypeDatasetInfo.systemId, prototypeVersionInfo.systemId, prototypeDatasetInfo.localeName, prototypeDatasetInfo.obfuscationKey)
    assert(vi.datasetInfo.unanchored == prototypeDatasetInfo)
    assert(vi.unanchored == prototypeVersionInfo)
    schemaLoader.create(vi)
    vi
  }

  def addColumn(currentVersion: CopyInfo, columnInfos: Iterable[UnanchoredColumnInfo]): currentVersion.type = {
    // TODO: Re-paranoid this
    // Resync.unless(currentVersion, currentVersion == columnInfo.copyInfo, "Copy infos differ")

    val cis = columnInfos.toVector.map { columnInfo =>
      val ci = datasetMap.addColumnWithId(columnInfo.systemId, currentVersion, columnInfo.userColumnId, typeNamespace.typeForName(currentVersion.datasetInfo, columnInfo.typeName), columnInfo.physicalColumnBaseBase)
      Resync.unless(ci, ci.unanchored == columnInfo, "Newly created column info differs")
      ci
    }
    schemaLoader.addColumns(cis)
    currentVersion
  }

  def dropColumn(currentVersion: CopyInfo, columnInfos: Iterable[UnanchoredColumnInfo]): currentVersion.type = {
    // TODO: Re-paranoid this
    // Resync.unless(currentVersion, currentVersion == columnInfo.copyInfo, "Version infos differ")
    val cis = columnInfos.toVector.map { columnInfo =>
      val ci = datasetMap.schema(currentVersion).getOrElse(columnInfo.systemId, Resync(currentVersion, "No column with ID " + columnInfo.systemId))
      Resync.unless(ci, ci.unanchored == columnInfo, "Column infos differ")
      datasetMap.dropColumn(ci)
      ci
    }
    schemaLoader.dropColumns(cis)
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

  def versionColumnChanged(currentVersionInfo: CopyInfo, columnInfo: UnanchoredColumnInfo): currentVersionInfo.type = {
    // TODO: Re-paranoid this
    // Resync.unless(currentVersionInfo, currentVersionInfo == columnInfo.copyInfo, "Version infos differ")
    val ci = datasetMap.schema(currentVersionInfo)(columnInfo.systemId)
    val ci2 = datasetMap.setVersion(ci)
    Resync.unless(ci2, ci2.unanchored == columnInfo, "Column infos differ:\n" + ci2.unanchored + "\n" + columnInfo)
    schemaLoader.makeVersion(ci2)
    currentVersionInfo
  }

  def lastModifiedChanged(currentVersionInfo: CopyInfo, lastModified: DateTime): currentVersionInfo.type = {
    Resync.unless(currentVersionInfo, currentVersionInfo.lastModified.isBefore(lastModified), "Told to send last modified back in time")
    datasetMap.updateLastModified(currentVersionInfo, lastModified)
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
        oldSchema.mapValuesStrict(_.unanchored.copy(isSystemPrimaryKey = false, isUserPrimaryKey = false, isVersion = false)) == newSchema.mapValuesStrict(_.unanchored),
        "published and unpublished schemas differ:\n" + oldSchema.mapValuesStrict(_.unanchored) + "\n" + newSchema.mapValuesStrict(_.unanchored))
    }

    contentsCopier.copy(oldVersion, new DatasetCopyContext(currentVersion, newSchema))

    currentVersion
  }

  def dropWorkingCopy(currentVersion: CopyInfo): CopyInfo = {
    Resync.unless(currentVersion, currentVersion.lifecycleStage == LifecycleStage.Unpublished, "Told to drop the current copy, but it isn't an unpublished copy")
    datasetMap.dropCopy(currentVersion)
    schemaLoader.drop(currentVersion)
    datasetMap.latest(currentVersion.datasetInfo)
  }

  def dropSnapshot(currentVersion: CopyInfo, droppedCopy: UnanchoredCopyInfo): currentVersion.type = {
    datasetMap.copyNumber(currentVersion.datasetInfo, droppedCopy.copyNumber) match {
      case Some(toDrop) =>
        Resync.unless(currentVersion, toDrop == droppedCopy, "Dropped copy didn't match")
        datasetMap.dropCopy(toDrop)
        schemaLoader.drop(toDrop)
        currentVersion
      case None =>
        Resync(currentVersion.datasetInfo, "Told to drop a copy that didn't exist")
    }
  }

  def truncate(currentVersion: CopyInfo): currentVersion.type = {
    truncate(currentVersion.dataTableName)
    currentVersion
  }

  def populateData(currentVersionInfo: CopyInfo, ops: Seq[Operation[SoQLValue]]): currentVersionInfo.type = {
    val loader = dataLoader(currentVersionInfo)
    ops.foreach {
      case Insert(sid, row) => loader.insert(sid, row)
      case Update(sid, oldRow, newRow) => loader.update(sid, oldRow, newRow)
      case Delete(sid, oldRow) => loader.delete(sid, oldRow)
    }
    loader.flush()
    currentVersionInfo
  }

  def counterUpdated(currentVersionInfo: CopyInfo, ctr: Long): CopyInfo = {
    logger.counterUpdated(ctr)
    datasetMap.updateNextCounterValue(currentVersionInfo, ctr)
  }

  def publish(currentVersionInfo: CopyInfo): CopyInfo =
    try {
      datasetMap.publish(currentVersionInfo)
    } catch {
      case _: IllegalArgumentException =>
        Resync(currentVersionInfo, "Not a published copy")
    }

  private[this] val pendingAdds = new ListBuffer[UnanchoredColumnInfo]
  private[this] val pendingDrops = new ListBuffer[UnanchoredColumnInfo]

  def dispatch(initVersionInfo: CopyInfo, event: Delogger.LogEvent[SoQLValue]): CopyInfo = {
    val versionInfo =
      if(pendingAdds.nonEmpty && !event.isInstanceOf[ColumnCreated]) {
        // type ascription asserts that the version info is not changed by the add column call
        val newvi: initVersionInfo.type = addColumn(initVersionInfo, pendingAdds)
        pendingAdds.clear()
        newvi
      } else if(pendingDrops.nonEmpty && !event.isInstanceOf[ColumnRemoved]) {
        // type ascription asserts that the version info is not changed by the add column call
        val newvi: initVersionInfo.type = dropColumn(initVersionInfo, pendingDrops)
        pendingDrops.clear()
        newvi
      } else {
        initVersionInfo
      }
    event match {
      case WorkingCopyCreated(_, newVersionInfo) =>
        makeWorkingCopy(versionInfo, newVersionInfo)
      case ColumnCreated(info) =>
        pendingAdds += info
        versionInfo
      case RowIdentifierSet(info) =>
        makePrimaryKey(versionInfo, info)
      case SystemRowIdentifierChanged(info) =>
        makeSystemPrimaryKey(versionInfo, info)
      case VersionColumnChanged(info) =>
        versionColumnChanged(versionInfo, info)
      case LastModifiedChanged(time) =>
        lastModifiedChanged(versionInfo, time)
      case RowIdentifierCleared(info) =>
        dropPrimaryKey(versionInfo, info)
      case r@RowDataUpdated(_) =>
        populateData(versionInfo, r.operations)
      case CounterUpdated(ctr) =>
        counterUpdated(versionInfo, ctr)
      case WorkingCopyPublished =>
        publish(versionInfo)
      case DataCopied =>
        populateWorkingCopy(versionInfo)
      case ColumnRemoved(info) =>
        pendingDrops += info
        versionInfo
      case Truncated =>
        truncate(versionInfo)
      case WorkingCopyDropped =>
        dropWorkingCopy(versionInfo)
      case SnapshotDropped(info) =>
        dropSnapshot(versionInfo, info)
      case EndTransaction =>
        versionInfo
    }
  }
}
