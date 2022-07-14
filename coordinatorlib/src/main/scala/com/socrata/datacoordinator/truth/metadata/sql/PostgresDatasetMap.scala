package com.socrata.datacoordinator
package truth.metadata
package sql

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReaderException}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.soql.environment.{ColumnName, ResourceName}
import scala.collection.immutable.VectorBuilder

import java.sql._

import org.postgresql.util.PSQLException
import com.rojoma.simplearm.v2.using

import com.socrata.datacoordinator.truth.{DatabaseInReadOnlyMode, DatasetIdInUseByWriterException}
import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.util.{TimingReport, PostgresUniqueViolation}
import com.socrata.datacoordinator.util.collection.{ColumnIdMap, MutableColumnIdMap}
import com.socrata.datacoordinator.truth.metadata.`-impl`._
import scala.concurrent.duration.Duration
import com.socrata.datacoordinator.id.sql._
import scala.Array
import scala.collection.mutable
import org.joda.time.DateTime

trait BasePostgresDatasetMapReader[CT] extends `-impl`.BaseDatasetMapReader[CT] {
  implicit def typeNamespace: TypeNamespace[CT]
  implicit def tag: Tag = null

  val conn: Connection
  def t: TimingReport

  private def toDateTime(time: Timestamp): DateTime = new DateTime(time.getTime)

  // The time the current transaction was started.
  def currentTime(): DateTime = {
    using(conn.prepareStatement("SELECT CURRENT_TIMESTAMP")) { stmt=>
      using(stmt.executeQuery()) { rs =>
        rs.next()
        toDateTime(rs.getTimestamp(1))
      }
    }
  }

  def snapshotCountQuery = "SELECT count(system_id) FROM copy_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)"
  def snapshotCount(dataset: DatasetInfo) =
    using(conn.prepareStatement(snapshotCountQuery)) { stmt =>
      stmt.setDatasetId(1, dataset.systemId)
      stmt.setString(2, LifecycleStage.Snapshotted.name)
      using(t("shapshot-count", "dataset_id" -> dataset.systemId)(stmt.executeQuery())) { rs =>
        rs.next()
        rs.getInt(1)
      }
    }

  // data_shape_version is nullable; if it's null it means this
  // dataset was created before that column was introduced and that
  // data_version should be used in its place.
  private def getDataShapeVersion(rs: ResultSet): Long = {
    val dsv = rs.getLong("data_shape_version")
    if(rs.wasNull) {
      rs.getLong("data_version")
    } else {
      dsv
    }
  }

  def latestQuery =
    """SELECT
      |  system_id, copy_number, lifecycle_stage :: TEXT, data_version, data_shape_version, last_modified, table_modifier
      |FROM
      |  copy_map
      |  LEFT OUTER JOIN copy_map_table_modifiers ON copy_map.system_id = copy_map_table_modifiers.copy_system_id
      |WHERE
      |  dataset_system_id = ?
      |  AND data_version <= ?
      |  AND lifecycle_stage <> 'Discarded'
      |ORDER BY
      |  data_version DESC, copy_number DESC
      |LIMIT 1""".stripMargin
  def latestUpTo(datasetInfo: DatasetInfo, dataVersion: Option[Long] = None) =
    using(conn.prepareStatement(latestQuery)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      stmt.setLong(2, dataVersion.getOrElse(Long.MaxValue))
      using(t("latest-copy", "dataset_id" -> datasetInfo.systemId)(stmt.executeQuery())) { rs =>
        if(!rs.next()) sys.error("Looked up a table for " + datasetInfo.systemId +
          " < version " + dataVersion.getOrElse(Long.MaxValue) + " but didn't find any copy info?")
        CopyInfo(
          datasetInfo,
          new CopyId(rs.getLong("system_id")),
          rs.getLong("copy_number"),
          LifecycleStage.valueOf(rs.getString("lifecycle_stage")),
          rs.getLong("data_version"),
          getDataShapeVersion(rs),
          toDateTime(rs.getTimestamp("last_modified")),
          rs.getNullableLong("table_modifier")
        )
      }
    }

  def latest(datasetInfo: DatasetInfo) = latestUpTo(datasetInfo, None)

  def allCopiesQuery =
    """SELECT
      |  system_id, copy_number, lifecycle_stage :: TEXT, data_version, data_shape_version, last_modified, table_modifier
      |FROM
      |  copy_map
      |  LEFT OUTER JOIN copy_map_table_modifiers ON copy_map.system_id = copy_map_table_modifiers.copy_system_id
      |WHERE
      |  dataset_system_id = ?
      |ORDER BY
      |  copy_number""".stripMargin
  def allCopies(datasetInfo: DatasetInfo): Vector[CopyInfo] =
    using(conn.prepareStatement(allCopiesQuery)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      using(t("all-copies", "dataset_id" -> datasetInfo.systemId)(stmt.executeQuery()))(readCopies(datasetInfo))
    }

  private def readCopies(datasetInfo: DatasetInfo)(rs: ResultSet): Vector[CopyInfo] = {
    val result = new VectorBuilder[CopyInfo]
    while(rs.next()) {
      result += CopyInfo(
        datasetInfo,
        new CopyId(rs.getLong("system_id")),
        rs.getLong("copy_number"),
        LifecycleStage.valueOf(rs.getString("lifecycle_stage")),
        rs.getLong("data_version"),
        getDataShapeVersion(rs),
        toDateTime(rs.getTimestamp("last_modified")),
        rs.getNullableLong("table_modifier")
      )
    }
    result.result()
  }

  def lookupQuery =
    """SELECT
      |  system_id, copy_number, data_version, data_shape_version, last_modified, table_modifier
      |FROM
      |  copy_map
      |  LEFT OUTER JOIN copy_map_table_modifiers ON copy_map.system_id = copy_map_table_modifiers.copy_system_id
      |WHERE
      |  dataset_system_id = ?
      |  AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)
      |ORDER BY
      |  data_version DESC, copy_number DESC limit 1""".stripMargin
  def lookup(datasetInfo: DatasetInfo, stage: LifecycleStage): Option[CopyInfo] = {
    using(conn.prepareStatement(lookupQuery)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      stmt.setString(2, stage.name)
      using(t("lookup-copy","dataset_id" -> datasetInfo.systemId,"lifecycle-stage"->stage)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(CopyInfo(datasetInfo, new CopyId(rs.getLong("system_id")), rs.getLong("copy_number"), stage, rs.getLong("data_version"), getDataShapeVersion(rs), toDateTime(rs.getTimestamp("last_modified")), rs.getNullableLong("table_modifier")))
        } else {
          None
        }
      }
    }
  }

  def lookupCopyQuery =
    """SELECT
      |  system_id, copy_number, lifecycle_stage, data_version, data_shape_version, last_modified, table_modifier
      |FROM
      |  copy_map
      |  LEFT OUTER JOIN copy_map_table_modifiers ON copy_map.system_id = copy_map_table_modifiers.copy_system_id
      |WHERE
      |  dataset_system_id = ?
      |  AND copy_number = ?
      |ORDER BY
      |  copy_number""".stripMargin
  def lookupCopy(datasetInfo: DatasetInfo, copyNumber: Long): Option[CopyInfo] = {
    using(conn.prepareStatement(lookupCopyQuery)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      stmt.setLong(2, copyNumber)
      using(t("lookup-copy","dataset_id" -> datasetInfo.systemId,"copy_number"->copyNumber)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(CopyInfo(datasetInfo, new CopyId(rs.getLong("system_id")), rs.getLong("copy_number"), rs.getLifecycleStage("lifecycle_stage"), rs.getLong("data_version"), getDataShapeVersion(rs), toDateTime(rs.getTimestamp("last_modified")), rs.getNullableLong("table_modifier")))
        } else {
          None
        }
      }
    }
  }

  def previousVersionQuery =
    """SELECT
      |  system_id, copy_number, lifecycle_stage :: TEXT, data_version, data_shape_version, last_modified, table_modifier
      |FROM
      |  copy_map
      |  LEFT OUTER JOIN copy_map_table_modifiers ON copy_map.system_id = copy_map_table_modifiers.copy_system_id
      |WHERE
      |  dataset_system_id = ?
      |  AND copy_number < ?
      |  AND lifecycle_stage <> 'Discarded'
      |ORDER BY
      |  copy_number DESC
      |LIMIT 1""".stripMargin
  def previousVersion(copyInfo: CopyInfo): Option[CopyInfo] = {
    using(conn.prepareStatement(previousVersionQuery)) { stmt =>
      stmt.setDatasetId(1, copyInfo.datasetInfo.systemId)
      stmt.setLong(2, copyInfo.copyNumber)
      using(t("previous-version","dataset_id" -> copyInfo.datasetInfo.systemId,"copy_num" -> copyInfo.copyNumber)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(CopyInfo(
            copyInfo.datasetInfo,
            new CopyId(rs.getLong("system_id")),
            rs.getLong("copy_number"),
            LifecycleStage.valueOf(rs.getString("lifecycle_stage")),
            rs.getLong("data_version"),
            getDataShapeVersion(rs),
            toDateTime(rs.getTimestamp("last_modified")),
            rs.getNullableLong("table_modifier")
          ))
        } else {
          None
        }
      }
    }
  }

  def copyNumberQuery =
    """SELECT
      |  system_id, lifecycle_stage, data_version, data_shape_version, last_modified, table_modifier
      |FROM
      |  copy_map
      |  LEFT OUTER JOIN copy_map_table_modifiers ON copy_map.system_id = copy_map_table_modifiers.copy_system_id
      |WHERE
      |  dataset_system_id = ?
      |  AND copy_number = ?""".stripMargin
  def copyNumber(datasetInfo: DatasetInfo, copyNumber: Long): Option[CopyInfo] =
    using(conn.prepareStatement(copyNumberQuery)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      stmt.setLong(2, copyNumber)
      using(t("copy-by-number", "dataset_id" -> datasetInfo.systemId, "copy_num" -> copyNumber)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(CopyInfo(
            datasetInfo,
            new CopyId(rs.getLong("system_id")),
            copyNumber,
            LifecycleStage.valueOf(rs.getString("lifecycle_stage")),
            rs.getLong("data_version"),
            getDataShapeVersion(rs),
            toDateTime(rs.getTimestamp("last_modified")),
            rs.getNullableLong("table_modifier")
          ))
        } else {
          None
        }
      }
    }

  def allDatasetsQuery = "SELECT system_id FROM dataset_map order by system_id"
  def allDatasetIds(): Seq[DatasetId] = {
    using(conn.prepareStatement(allDatasetsQuery)) { stmt =>
      using(t("all-datasets")(stmt.executeQuery())) { rs =>
        val res = new VectorBuilder[DatasetId]
        while(rs.next()) {
          res += rs.getDatasetId(1)
        }
        res.result()
      }
    }
  }

  def schemaQuery = "SELECT system_id, user_column_id, field_name, type_name, physical_column_base_base, (is_system_primary_key IS NOT NULL) is_system_primary_key, (is_user_primary_key IS NOT NULL) is_user_primary_key, (is_version IS NOT NULL) is_version FROM column_map WHERE copy_system_id = ?;" +
    "SELECT column_system_id, strategy_type, source_column_ids, parameters FROM computation_strategy_map WHERE copy_system_id = ?;" +
    "SELECT column_system_id, zoom_level from column_map_geo_modifiers where copy_system_id = ?;"
  def schema(copyInfo: CopyInfo): ColumnIdMap[ColumnInfo[CT]] = {
    using(conn.prepareStatement(schemaQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.setLong(2, copyInfo.systemId.underlying)
      stmt.setLong(3, copyInfo.systemId.underlying)

      val result = new MutableColumnIdMap[ColumnInfo[CT]]
      t("schema-lookup", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber)(stmt.execute())
      using(stmt.getResultSet) { rs =>
        while (rs.next()) {
          val systemId = rs.getColumnId("system_id")
          // TODO: Proper getters!
          val columnName = new UserColumnId(rs.getString("user_column_id"))
          val fieldName = Option(rs.getString("field_name")).map(new ColumnName(_))
          result += systemId -> ColumnInfo(
            copyInfo,
            systemId,
            columnName,
            fieldName,
            typeNamespace.typeForName(copyInfo.datasetInfo, rs.getString("type_name")),
            rs.getString("physical_column_base_base"),
            rs.getBoolean("is_system_primary_key"),
            rs.getBoolean("is_user_primary_key"),
            rs.getBoolean("is_version"),
            None,
            List.empty)
        }
      }

      if (!stmt.getMoreResults()) sys.error("I issued three queries, why don't I have two resultSets?")
      using(stmt.getResultSet) { rs =>
        while (rs.next()) {
          val systemId = rs.getColumnId("column_system_id")
          // TODO: Proper getters!
          val strategyType = new StrategyType(rs.getString("strategy_type"))
          val sourceColumnIds = rs.getArray("source_column_ids").getArray.asInstanceOf[Array[String]].map(new UserColumnId(_))
          val parameters = try {
            JsonUtil.parseJson[JObject](rs.getString("parameters")).right.getOrElse {
              sys.error("Invalid data in the database: the computation strategy parameters for the column " + systemId + " on copy " + copyInfo.copyNumber + " of dataset " + copyInfo.datasetInfo.systemId + " is valid JSON but not an object")
            }
          } catch {
            case _: JsonReaderException =>
              sys.error("Invalid data in the database: the computation strategy parameters for the column " + systemId + " on copy " + copyInfo.copyNumber + " of dataset " + copyInfo.datasetInfo.systemId + " is not valid JSON")
          }
          val csi = new ComputationStrategyInfo(strategyType, sourceColumnIds, parameters)
          result(systemId) = result(systemId).copy(computationStrategyInfo = Some(csi))
        }
      }

      if (!stmt.getMoreResults()) sys.error("I issued three queries, why don't I have three resultSets?")
      using(stmt.getResultSet) { rs =>
        while (rs.next()) {
          val systemId = rs.getColumnId("column_system_id")
          val zoomLevel = rs.getInt("zoom_level")

          val levels = zoomLevel +: result(systemId).presimplifiedZoomLevels
          result(systemId) = result(systemId).copy(presimplifiedZoomLevels = levels)
        }
      }

      result.freeze()
    }
  }

  def rollupsQuery = "SELECT name, soql, raw_soql FROM rollup_map WHERE copy_system_id = ?"
  def rollups(copyInfo: CopyInfo): Seq[RollupInfo] = {
    using(conn.prepareStatement(rollupsQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      using(t("rollups", "copy_id" -> copyInfo.systemId)(stmt.executeQuery())) { rs =>
        val res = new VectorBuilder[RollupInfo]
        while(rs.next()) {
          res += RollupInfo(copyInfo, new RollupName(rs.getString("name")), rs.getString("soql"), Option(rs.getString("soql")))
        }
        res.result()
      }
    }
  }

  def rollupQuery = "SELECT soql, raw_soql FROM rollup_map WHERE copy_system_id = ? AND name = ?"
  def rollup(copyInfo: CopyInfo, name: RollupName): Option[RollupInfo] = {
    using(conn.prepareStatement(rollupQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.setString(2, name.underlying)
      using(t("rollup", "copy_id" -> copyInfo.systemId,"name" -> name)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(RollupInfo(copyInfo, name, rs.getString("soql"), Option(rs.getString("raw_soql"))))
        } else {
          None
        }
      }
    }
  }

  def indexesQuery = "SELECT system_id, name, expressions, filter FROM index_map WHERE copy_system_id = ? AND deleted_at is null"
  def indexes(copyInfo: CopyInfo): Seq[IndexInfo] = {
    using(conn.prepareStatement(indexesQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      using(t("indexes", "copy_id" -> copyInfo.systemId)(stmt.executeQuery())) { rs =>
        val res = new VectorBuilder[IndexInfo]
        while(rs.next()) {
          res += IndexInfo(rs.getIndexId("system_id"), copyInfo, new IndexName(rs.getString("name")),
            rs.getString("expressions"), Option(rs.getString("filter")))
        }
        res.result()
      }
    }
  }

  def indexQuery = "SELECT system_id, expressions, filter FROM index_map WHERE copy_system_id = ? AND name = ? AND deleted_at is null"
  def index(copyInfo: CopyInfo, name: IndexName): Option[IndexInfo] = {
    using(conn.prepareStatement(indexQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.setString(2, name.underlying)
      using(t("index", "copy_id" -> copyInfo.systemId,"name" -> name)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(IndexInfo(rs.getIndexId("system_id"), copyInfo, name, rs.getString("expressions"), Option(rs.getString("filter"))))
        } else {
          None
        }
      }
    }
  }

  // These are from the reader trait but they're used in the writer tests
  def unpublished(datasetInfo: DatasetInfo) =
    lookup(datasetInfo, LifecycleStage.Unpublished)

  def published(datasetInfo: DatasetInfo) =
    lookup(datasetInfo, LifecycleStage.Published)

  def snapshot(datasetInfo: DatasetInfo, copyNumber: Long) =
    lookupCopy(datasetInfo, copyNumber).filter(_.lifecycleStage == LifecycleStage.Snapshotted)

  def snapshotsQuery =
    """SELECT
      |  system_id, copy_number, lifecycle_stage :: TEXT, data_version, data_shape_version, last_modified, table_modifier
      |FROM
      |  copy_map
      |  LEFT OUTER JOIN copy_map_table_modifiers ON copy_map.system_id = copy_map_table_modifiers.copy_system_id
      |WHERE
      |  dataset_system_id = ?
      |  AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)
      |ORDER BY
      |  copy_number""".stripMargin
  def snapshots(datasetInfo: DatasetInfo): Vector[CopyInfo] =
    using(conn.prepareStatement(snapshotsQuery)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      stmt.setString(2, LifecycleStage.Snapshotted.name)
      using(t("snapshots", "dataset_id" -> datasetInfo.systemId)(stmt.executeQuery()))(readCopies(datasetInfo))
    }

  private def indexDirectivesQuery =
    "SELECT column_system_id, directive FROM index_directive_map WHERE copy_system_id = ? AND deleted_at is null"
  private def indexDirectiveQuery =
    """SELECT column_system_id, directive FROM index_directive_map idm
         JOIN column_map cm ON cm.copy_system_id=idm.copy_system_id AND cm.system_id=idm.column_system_id
        WHERE idm.copy_system_id = ?
          AND cm.field_name = ?
          AND idm.deleted_at is null"""
  def indexDirectives(copyInfo: CopyInfo, fieldName: Option[ColumnName]): Seq[IndexDirective[CT]] = {
    val columnIdMap = schema(copyInfo)
    val query = if (fieldName.isDefined) indexDirectiveQuery else indexDirectivesQuery
    using(conn.prepareStatement(query)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      for (name <- fieldName) {
        stmt.setString(2, name.name)
      }
      using(stmt.executeQuery()) { rs =>
        val res = new VectorBuilder[IndexDirective[CT]]
        while(rs.next()) {
          JsonUtil.parseJson[JObject](rs.getString("directive")) match {
            case Right(jo) =>
              val columnId = rs.getColumnId("column_system_id")
              res += IndexDirective(copyInfo, columnIdMap(columnId), jo)
            case _ =>
          }
        }
        res.result()
      }
    }
  }
}

class PostgresDatasetMapReader[CT](val conn: Connection, tns: TypeNamespace[CT], timingReport: TimingReport) extends DatasetMapReader[CT] with BasePostgresDatasetMapReader[CT] {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[BasePostgresDatasetMapReader[_]])

  implicit def typeNamespace = tns
  def t = timingReport

  def datasetInfoBySystemIdQuery = "SELECT system_id, next_counter_value, latest_data_version, locale_name, obfuscation_key, resource_name FROM dataset_map WHERE system_id = ?"
  def datasetInfo(datasetId: DatasetId, repeatableRead: Boolean = false) = {
    if (repeatableRead) {
      log.info("Attempting to change transaction isolation level...")
      conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      log.info("Changed transaction isolation level to REPEATABLE READ")
    }
    using(conn.prepareStatement(datasetInfoBySystemIdQuery)) { stmt =>
      stmt.setDatasetId(1, datasetId)
      using(t("lookup-dataset", "dataset_id" -> datasetId)(stmt.executeQuery())) { rs =>
        if (rs.next()) {
          Some(DatasetInfo(rs.getDatasetId("system_id"), rs.getLong("next_counter_value"), rs.getLong("latest_data_version"), rs.getString("locale_name"), rs.getBytes("obfuscation_key"), Option(rs.getString("resource_name"))))
        } else {
          None
        }
      }
    }
  }

  def datasetInfoByResourceNameQuery = "SELECT system_id, next_counter_value, latest_data_version, locale_name, obfuscation_key, resource_name FROM dataset_map WHERE resource_name = ?"
  def datasetInfoByResourceName(resourceName: ResourceName, repeatableRead: Boolean = false) = {
    if (repeatableRead) {
      log.info("Attempting to change transaction isolation level...")
      conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      log.info("Changed transaction isolation level to REPEATABLE READ")
    }
    using(conn.prepareStatement(datasetInfoByResourceNameQuery)) { stmt =>
      stmt.setString(1, resourceName.name)
      using(t("lookup-dataset", "resource_name" -> resourceName)(stmt.executeQuery())) { rs =>
        if (rs.next()) {
          Some(DatasetInfo(rs.getDatasetId("system_id"), rs.getLong("next_counter_value"), rs.getLong("latest_data_version"), rs.getString("locale_name"), rs.getBytes("obfuscation_key"), Option(rs.getString("resource_name"))))
        } else {
          None
        }
      }
    }
  }

  def snapshottedDatasetsQuery = "SELECT distinct ds.system_id, ds.next_counter_value, ds.latest_data_version, ds.locale_name, ds.obfuscation_key, ds.resource_name FROM dataset_map ds JOIN copy_map c ON c.dataset_system_id = ds.system_id WHERE c.lifecycle_stage = 'Snapshotted'"
  def snapshottedDatasets() = {
    using(conn.prepareStatement(snapshottedDatasetsQuery)) { stmt =>
      using(t("snapshotted-datasets")(stmt.executeQuery())) { rs =>
        val result = Seq.newBuilder[DatasetInfo]
        while(rs.next()) {
          result += DatasetInfo(rs.getDatasetId("system_id"), rs.getLong("next_counter_value"), rs.getLong("latest_data_version"), rs.getString("locale_name"), rs.getBytes("obfuscation_key"), Option(rs.getString("resource_name")))
        }
        result.result()
      }
    }
  }
}

trait BasePostgresDatasetMapWriter[CT] extends BasePostgresDatasetMapReader[CT] with `-impl`.BaseDatasetMapWriter[CT] with IndexControl[CT] {
  val obfuscationKeyGenerator: () => Array[Byte]
  val initialCounterValue: Long
  val initialLatestDataVersion: Long

  private def toTimestamp(time: DateTime): Timestamp = new Timestamp(time.getMillis)

  def createQuery_tableMap = "INSERT INTO dataset_map (next_counter_value, latest_data_version, locale_name, obfuscation_key, resource_name) VALUES (?, ?, ?, ?, ?) RETURNING system_id"
  def createQuery_copyMap = "INSERT INTO copy_map (dataset_system_id, copy_number, lifecycle_stage, data_version, data_shape_version, last_modified) VALUES (?, ?, CAST(? AS dataset_lifecycle_stage), ?, ?, ?) RETURNING system_id"
  def create(localeName: String, resourceName: Option[String]): CopyInfo = {
    val datasetInfo = using(conn.prepareStatement(createQuery_tableMap)) { stmt =>
      val datasetInfoNoSystemId = DatasetInfo(DatasetId.Invalid, initialCounterValue, initialLatestDataVersion, localeName, obfuscationKeyGenerator(), resourceName)
      stmt.setLong(1, datasetInfoNoSystemId.nextCounterValue)
      stmt.setLong(2, datasetInfoNoSystemId.latestDataVersion)
      stmt.setString(3, datasetInfoNoSystemId.localeName)
      stmt.setBytes(4, datasetInfoNoSystemId.obfuscationKey)
      stmt.setString(5, datasetInfoNoSystemId.resourceName.orNull) // resource_name is a nullable column
      try {
        using(t("create-dataset") { stmt.executeQuery() }) { rs =>
          val returnedSomething = rs.next()
          assert(returnedSomething, "INSERT didn't return a system ID?")
          datasetInfoNoSystemId.copy(systemId = rs.getDatasetId(1))
        }
      } catch {
        case e: PSQLException if isReadOnlyTransaction(e) =>
          BasePostgresDatasetMapWriter.log.trace("Create dataset failed due to read-only txn; abandoning")
          throw new DatabaseInReadOnlyMode(e)
      }
    }

    using(conn.prepareStatement(createQuery_copyMap)) { stmt =>
      val copyInfoNoSystemId = CopyInfo(datasetInfo, new CopyId(-1), 1, LifecycleStage.Unpublished, 0, 0, DateTime.now, None)

      stmt.setDatasetId(1, copyInfoNoSystemId.datasetInfo.systemId)
      stmt.setLong(2, copyInfoNoSystemId.copyNumber)
      stmt.setString(3, copyInfoNoSystemId.lifecycleStage.name)
      stmt.setLong(4, copyInfoNoSystemId.dataVersion)
      stmt.setLong(5, copyInfoNoSystemId.dataShapeVersion)
      stmt.setTimestamp(6, toTimestamp(copyInfoNoSystemId.lastModified))
      using(t("create-initial-copy", "dataset_id" -> datasetInfo.systemId)(stmt.executeQuery())) { rs =>
        val foundSomething = rs.next()
        assert(foundSomething, "Didn't return a system ID?")
        copyInfoNoSystemId.copy(systemId = new CopyId(rs.getLong(1)))
      }
    }
  }

  def createQuery_copyMapWithSystemId = "INSERT INTO copy_map (system_id, dataset_system_id, copy_number, lifecycle_stage, data_version, data_shape_version, last_modified) VALUES (?, ?, ?, CAST(? AS dataset_lifecycle_stage), ?, ?, ?)"
  def createWithId(systemId: DatasetId, initialCopyId: CopyId, localeName: String, obfuscationKey: Array[Byte], resourceName: Option[String]): CopyInfo = {
    val datasetInfo = unsafeCreateDataset(systemId, initialCounterValue, initialLatestDataVersion, localeName, obfuscationKey, resourceName)

    using(conn.prepareStatement(createQuery_copyMapWithSystemId)) { stmt =>
      val copyInfo = CopyInfo(datasetInfo, initialCopyId, 1, LifecycleStage.Unpublished, 0, 0, DateTime.now, None)

      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.setDatasetId(2, copyInfo.datasetInfo.systemId)
      stmt.setLong(3, copyInfo.copyNumber)
      stmt.setString(4, copyInfo.lifecycleStage.name)
      stmt.setLong(5, copyInfo.dataVersion)
      stmt.setLong(6, copyInfo.dataShapeVersion)
      stmt.setTimestamp(7, toTimestamp(copyInfo.lastModified))
      try {
        t("create-create-copy-with-system-id", "dataset_id" -> systemId, "copy_id" -> initialCopyId)(stmt.execute())
      } catch {
        case PostgresUniqueViolation("system_id") =>
          throw new CopySystemIdAlreadyInUse(initialCopyId)
      }

      copyInfo
    }
  }

  // Yay no "DELETE ... CASCADE"!
  def deleteQuery_computationStrategyMap = "DELETE FROM computation_strategy_map WHERE copy_system_id IN (SELECT system_id FROM copy_map WHERE dataset_system_id = ?)"
  def deleteQuery_indexDirecitveMap = "DELETE FROM index_directive_map WHERE copy_system_id IN (SELECT system_id FROM copy_map WHERE dataset_system_id = ?)"
  def deleteQuery_indexMap = "DELETE FROM index_map WHERE copy_system_id IN (SELECT system_id FROM copy_map WHERE dataset_system_id = ?)"
  def deleteQuery_columnMap = "DELETE FROM column_map WHERE copy_system_id IN (SELECT system_id FROM copy_map WHERE dataset_system_id = ?)"
  def deleteQuery_rollupMap = "DELETE FROM rollup_map WHERE copy_system_id IN (SELECT system_id FROM copy_map WHERE dataset_system_id = ?)"
  def deleteQuery_copyMap = "DELETE FROM copy_map WHERE dataset_system_id = ?"
  def deleteQuery_copyMapTableModifiers = "DELETE FROM copy_map_table_modifiers WHERE copy_system_id in (SELECT system_id FROM copy_map WHERE dataset_system_id = ?)"
  def deleteQuery_secondaryMoveJobs = "DELETE FROM secondary_move_jobs WHERE dataset_system_id = ?"
  def deleteQuery_tableMap = "DELETE FROM dataset_map WHERE system_id = ?"
  def deleteQuery_resync = "DELETE FROM resync WHERE dataset_system_id = ?"
  def delete(tableInfo: DatasetInfo) {
    deleteCopiesOf(tableInfo)
    using(conn.prepareStatement(deleteQuery_resync)) { stmt =>
      stmt.setDatasetId(1, tableInfo.systemId)
      t("delete-dataset-resync", "dataset_id" -> tableInfo.systemId)(stmt.executeUpdate())
    }
    using(conn.prepareStatement(deleteQuery_secondaryMoveJobs)) { stmt =>
      stmt.setDatasetId(1, tableInfo.systemId)
      t("delete-dataset-secondary-move-jobs", "dataset_id" -> tableInfo.systemId)(stmt.executeUpdate())
    }
    using(conn.prepareStatement(deleteQuery_tableMap)) { stmt =>
      stmt.setDatasetId(1, tableInfo.systemId)
      val count = t("delete-dataset", "dataset_id" -> tableInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Called delete on a table which is no longer there?")
    }
  }

  def deleteCopiesOf(datasetInfo: DatasetInfo) {
    using(conn.prepareStatement(deleteQuery_computationStrategyMap)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      t("delete-dataset-computation-strategies", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
    }
    using(conn.prepareStatement(deleteQuery_indexDirecitveMap)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      t("delete-index-directives", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
    }
    using(conn.prepareStatement(deleteQuery_indexMap)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      t("delete-indexes", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
    }
    using(conn.prepareStatement(deleteQuery_columnMap)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      t("delete-dataset-columns", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
    }
    using(conn.prepareStatement(deleteQuery_rollupMap)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      t("delete-dataset-rollups", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
    }
    using(conn.prepareStatement(deleteQuery_copyMapTableModifiers)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      t("delete-dataset-copies", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
    }
    using(conn.prepareStatement(deleteQuery_copyMap)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      t("delete-dataset-copies", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
    }
  }

  // like file descriptors, new columns always get the smallest available ID.  But "smallest available"
  // means "not used by this version OR THE PREVIOUS VERSION" so we can track column identity across
  // publication cycles.
  def findFirstFreeColumnIdQuery =
    """-- Adapted from http://johtopg.blogspot.com/2010/07/smallest-available-id.html
      |-- Use zero if available
      |(SELECT
      |    0 AS next_system_id
      | WHERE
      |    NOT EXISTS
      |        (SELECT 1 FROM column_map WHERE system_id = 0 AND (copy_system_id = ? OR copy_system_id = ?)) )
      |
      |    UNION ALL
      |
      |-- Find the smallest available ID inside a gap, or max + 1
      |-- if there are no gaps.
      |(SELECT
      |    system_id + 1 AS next_system_id
      | FROM
      | (
      |    SELECT
      |        system_id, lead(system_id) OVER (ORDER BY system_id)
      |    FROM
      |        column_map
      |    WHERE
      |        (copy_system_id = ? OR copy_system_id = ?)
      | ) ss
      | WHERE
      |    lead - system_id > 1 OR
      |    lead IS NULL
      | ORDER BY
      |    system_id
      | LIMIT
      |    1)
      |
      |ORDER BY
      |    next_system_id
      |LIMIT
      |    1
      |""".stripMargin
  def findFirstFreeColumnId(copyInfoA: CopyInfo, copyInfoB: CopyInfo): ColumnId = {
    using(conn.prepareStatement(findFirstFreeColumnIdQuery)) { stmt =>
      stmt.setLong(1, copyInfoA.systemId.underlying)
      stmt.setLong(2, copyInfoB.systemId.underlying)
      stmt.setLong(3, copyInfoA.systemId.underlying)
      stmt.setLong(4, copyInfoB.systemId.underlying)
      using(t("find-first-free-column-id", "dataset_id" -> copyInfoA.datasetInfo.systemId, "copy_num_a" -> copyInfoA.copyNumber, "copy_num_b" -> copyInfoB.copyNumber)(stmt.executeQuery())) { rs =>
        val foundSomething = rs.next()
        assert(foundSomething, "Finding the last column info didn't return anything?")
        new ColumnId(rs.getLong("next_system_id"))
      }
    }
  }

  def addColumn(copyInfo: CopyInfo, userColumnId: UserColumnId, fieldName: Option[ColumnName], typ: CT, physicalColumnBaseBase: String, computationStrategyInfo: Option[ComputationStrategyInfo] = None): ColumnInfo[CT] = {
    val systemId =
      previousVersion(copyInfo) match {
        case Some(previousCopy) =>
          findFirstFreeColumnId(copyInfo, previousCopy)
        case None =>
          findFirstFreeColumnId(copyInfo, copyInfo)
      }

    addColumnWithId(systemId, copyInfo, userColumnId, fieldName, typ, physicalColumnBaseBase, computationStrategyInfo)
  }

  def addColumnQuery = "INSERT INTO column_map (system_id, copy_system_id, user_column_id, field_name, field_name_casefolded, type_name, physical_column_base_base) VALUES (?, ?, ?, ?, ?, ?, ?)"
  def addComputationStrategyQuery = "INSERT INTO computation_strategy_map (column_system_id, copy_system_id, strategy_type, source_column_ids, parameters) VALUES (?, ?, ?, ?, ?)"
  def addColumnWithId(systemId: ColumnId, copyInfo: CopyInfo, userColumnId: UserColumnId, fieldName: Option[ColumnName], typ: CT, physicalColumnBaseBase: String, computationStrategyInfo: Option[ComputationStrategyInfo] = None): ColumnInfo[CT] = {
    val columnInfo = ColumnInfo[CT](copyInfo, systemId, userColumnId, fieldName, typ, physicalColumnBaseBase, isSystemPrimaryKey = false, isUserPrimaryKey = false, isVersion = false, computationStrategyInfo, Seq.empty)

    val result = using(conn.prepareStatement(addColumnQuery)) { stmt =>
      stmt.setLong(1, columnInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.copyInfo.systemId.underlying)
      stmt.setString(3, userColumnId.underlying)
      stmt.setString(4, fieldName.map(_.name).orNull)
      stmt.setString(5, fieldName.map(_.caseFolded).orNull)
      stmt.setString(6, typeNamespace.nameForType(typ))
      stmt.setString(7, physicalColumnBaseBase)
      try {
        t("add-column-with-id", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber, "column_id" -> systemId)(stmt.execute())
      } catch {
        case PostgresUniqueViolation("copy_system_id", "system_id") =>
          throw new ColumnSystemIdAlreadyInUse(copyInfo, systemId)
        case PostgresUniqueViolation("copy_system_id", "user_column_id") =>
          throw new ColumnAlreadyExistsException(copyInfo, userColumnId)
        case PostgresUniqueViolation("copy_system_id", "field_name_casefolded") =>
          throw new FieldNameAlreadyInUse(copyInfo, fieldName.get /* Won't have gotten this error without the FN being set */)
      }
    }

    computationStrategyInfo.foreach { strategy =>
      val ComputationStrategyInfo(strategyType, sourceColumnIds, parameters) = strategy

      using(conn.prepareStatement(addComputationStrategyQuery)) { stmt =>
        stmt.setLong(1, columnInfo.systemId.underlying)
        stmt.setLong(2, columnInfo.copyInfo.systemId.underlying)
        stmt.setString(3, strategyType.underlying)
        stmt.setArray(4, conn.createArrayOf("varchar", sourceColumnIds.map(_.underlying).toArray))
        stmt.setString(5, CompactJsonWriter.toString(parameters))
        try {
          t("add-column-computation-strategy-with-id", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber, "column_id" -> systemId)(stmt.execute())
        } catch {
          case PostgresUniqueViolation("copy_system_id", "column_system_id") =>
            throw new ColumnSystemIdAlreadyInUse(copyInfo, systemId)
        }
      }
    }

    columnInfo
  }

  def unsafeCreateDatasetQuery = "INSERT INTO dataset_map (system_id, next_counter_value, latest_data_version, locale_name, obfuscation_key, resource_name) VALUES (?, ?, ?, ?, ?, ?)"
  def unsafeCreateDataset(systemId: DatasetId, nextCounterValue: Long, latestDataVersion: Long, localeName: String, obfuscationKey: Array[Byte], resourceName: Option[String]): DatasetInfo = {
    val datasetInfo = DatasetInfo(systemId, nextCounterValue, latestDataVersion, localeName, obfuscationKey, resourceName)

    using(conn.prepareStatement(unsafeCreateDatasetQuery)) { stmt =>
      stmt.setDatasetId(1, datasetInfo.systemId)
      stmt.setLong(2, datasetInfo.nextCounterValue)
      stmt.setString(3, datasetInfo.localeName)
      stmt.setBytes(4, datasetInfo.obfuscationKey)
      stmt.setString(5, datasetInfo.resourceName.orNull) // resource_name is a nullable column
      try {
        t("unsafe-create-dataset", "dataset_id" -> systemId)(stmt.execute())
      } catch {
        case PostgresUniqueViolation("system_id") =>
          throw new DatasetSystemIdAlreadyInUse(systemId)
      }
    }

    datasetInfo
  }

  def unsafeCreateDatasetAllocatingSystemId(localeName: String, obfuscationKey: Array[Byte], resourceName: Option[String]): DatasetInfo = {
    using(conn.prepareStatement(createQuery_tableMap)) { stmt =>
      val datasetInfoNoSystemId = DatasetInfo(DatasetId.Invalid, initialCounterValue, initialLatestDataVersion, localeName, obfuscationKey, resourceName)
      stmt.setLong(1, datasetInfoNoSystemId.nextCounterValue)
      stmt.setLong(2, datasetInfoNoSystemId.latestDataVersion)
      stmt.setString(3, datasetInfoNoSystemId.localeName)
      stmt.setBytes(4, datasetInfoNoSystemId.obfuscationKey)
      stmt.setString(5, datasetInfoNoSystemId.resourceName.orNull) // resource_name is a nullable column
      try {
        using(t("unsafe-create-dataset-allocating-system-id") { stmt.executeQuery() }) { rs =>
          val returnedSomething = rs.next()
          assert(returnedSomething, "INSERT didn't return a system ID?")
          datasetInfoNoSystemId.copy(systemId = rs.getDatasetId(1))
        }
      } catch {
        case e: PSQLException if isReadOnlyTransaction(e) =>
          BasePostgresDatasetMapWriter.log.trace("Create dataset failed due to read-only txn; abandoning")
          throw new DatabaseInReadOnlyMode(e)
      }
    }
  }

  val unsafeReloadDatasetQuery = "UPDATE dataset_map SET next_counter_value = ?, latest_data_version = ?, locale_name = ?, obfuscation_key = ?, resource_name = ? WHERE system_id = ?"
  def unsafeReloadDataset(datasetInfo: DatasetInfo,
                          nextCounterValue: Long,
                          nextLatestDataVersion: Long,
                          localeName: String,
                          obfuscationKey: Array[Byte],
                          resourceName: Option[String]): DatasetInfo = {
    val newDatasetInfo = DatasetInfo(datasetInfo.systemId, nextCounterValue, nextLatestDataVersion, localeName, obfuscationKey, resourceName)
    using(conn.prepareStatement(unsafeReloadDatasetQuery)) { stmt =>
      stmt.setLong(1, newDatasetInfo.nextCounterValue)
      stmt.setLong(2, newDatasetInfo.latestDataVersion)
      stmt.setDatasetId(3, newDatasetInfo.systemId)
      stmt.setString(4, newDatasetInfo.localeName)
      stmt.setBytes(5, newDatasetInfo.obfuscationKey)
      stmt.setString(6, newDatasetInfo.resourceName.orNull) // resource_name is a nullable column
      val updated = t("unsafe-reload-dataset", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
      assert(updated == 1, s"Dataset ${datasetInfo.systemId.underlying} does not exist?")
    }

    deleteCopiesOf(newDatasetInfo)

    newDatasetInfo
  }

  def unsafeCreateCopyQuery = "INSERT INTO copy_map (system_id, dataset_system_id, copy_number, lifecycle_stage, data_version, data_shape_version, last_modified) values (?, ?, ?, CAST(? AS dataset_lifecycle_stage), ?, ?, ?)"
  def unsafeCreateCopy(datasetInfo: DatasetInfo,
                       systemId: CopyId,
                       copyNumber: Long,
                       lifecycleStage: LifecycleStage,
                       dataVersion: Long,
                       dataShapeVersion: Long): CopyInfo = {
    val newCopy = CopyInfo(datasetInfo, systemId, copyNumber, lifecycleStage, dataVersion, dataShapeVersion, DateTime.now, None)

    using(conn.prepareStatement(unsafeCreateCopyQuery)) { stmt =>
      stmt.setLong(1, newCopy.systemId.underlying)
      stmt.setDatasetId(2, newCopy.datasetInfo.systemId)
      stmt.setLong(3, newCopy.copyNumber)
      stmt.setString(4, newCopy.lifecycleStage.name)
      stmt.setLong(5, newCopy.dataVersion)
      stmt.setLong(6, newCopy.dataShapeVersion)
      stmt.setTimestamp(7, toTimestamp(newCopy.lastModified))
      try {
        t("unsafe-create-copy", "dataset_id" -> datasetInfo.systemId, "copy_num" -> copyNumber)(stmt.execute())
      } catch {
        case PostgresUniqueViolation("system_id") =>
          throw new CopySystemIdAlreadyInUse(systemId)
      }
    }

    newCopy
  }

  def dropColumnQuery = "DELETE FROM column_map WHERE copy_system_id = ? AND system_id = ?"
  def dropColumn(columnInfo: ColumnInfo[CT]) {
    columnInfo.computationStrategyInfo.foreach{ _ => dropComputationStrategy(columnInfo) }
    dropIndexDirective(columnInfo)
    using(conn.prepareStatement(dropColumnQuery)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      val count = t("drop-column", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Column did not exist to be dropped?")
    }
  }

  def addComputationStrategy(columnInfo: ColumnInfo[CT], strategy: ComputationStrategyInfo): ColumnInfo[CT] = {
    using(conn.prepareStatement(addComputationStrategyQuery)) { stmt =>
      val copyInfo = columnInfo.copyInfo
      val ComputationStrategyInfo(strategyType, sourceColumnIds, parameters) = strategy

      using(conn.prepareStatement(addComputationStrategyQuery)) { stmt =>
        stmt.setLong(1, columnInfo.systemId.underlying)
        stmt.setLong(2, columnInfo.copyInfo.systemId.underlying)
        stmt.setString(3, strategyType.underlying)
        stmt.setArray(4, conn.createArrayOf("varchar", sourceColumnIds.map(_.underlying).toArray))
        stmt.setString(5, CompactJsonWriter.toString(parameters))
        try {
          t("add-column-computation-strategy-with-id", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.execute())
        } catch {
          case PostgresUniqueViolation("copy_system_id", "column_system_id") =>
            throw new ColumnSystemIdAlreadyInUse(copyInfo, columnInfo.systemId)
        }
      }

      columnInfo.copy(computationStrategyInfo = Some(strategy))
    }
  }

  def dropComputationStrategyQuery = "DELETE FROM computation_strategy_map WHERE copy_system_id = ? AND column_system_id = ?"
  def dropComputationStrategy(columnInfo: ColumnInfo[CT]): ColumnInfo[CT] = {
    using(conn.prepareStatement(dropComputationStrategyQuery)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      val count = t("drop-computation-strategy", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Computation strategy did not exist to be dropped?")
      columnInfo.copy(computationStrategyInfo = None)
    }
  }

  def updateFieldNameQuery = "UPDATE column_map SET field_name = ?, field_name_casefolded = ? WHERE copy_system_id = ? AND system_id = ?"
  def updateFieldName(columnInfo: ColumnInfo[CT], newName: ColumnName): ColumnInfo[CT] = {
    using(conn.prepareStatement(updateFieldNameQuery)) { stmt =>
      stmt.setString(1, newName.name)
      stmt.setString(2, newName.caseFolded)
      stmt.setLong(3, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(4, columnInfo.systemId.underlying)
      val count = try {
        t("update-field-name", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      } catch {
        case PostgresUniqueViolation("copy_system_id", "field_name_casefolded") =>
          throw new FieldNameAlreadyInUse(columnInfo.copyInfo, newName)
      }
      assert(count == 1, "Column did not exist to have its field name updated?")
      columnInfo.copy(fieldName = Some(newName))
    }
  }

  def setSystemPrimaryKeyQuery = "UPDATE column_map SET is_system_primary_key = 'Unit' WHERE copy_system_id = ? AND system_id = ?"
  def setSystemPrimaryKey(columnInfo: ColumnInfo[CT]) =
    using(conn.prepareStatement(setSystemPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      val count = t("set-system-primary-key", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Column did not exist to have it set as primary key?")
      columnInfo.copy(isSystemPrimaryKey = true)
    }

  def setVersionQuery = "UPDATE column_map SET is_version = 'Unit' WHERE copy_system_id = ? AND system_id = ?"
  def setVersion(columnInfo: ColumnInfo[CT]) =
    using(conn.prepareStatement(setVersionQuery)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      val count = t("set-version", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Column did not exist to have it set as version?")
      columnInfo.copy(isVersion = true)
    }

  def setUserPrimaryKeyQuery = "UPDATE column_map SET is_user_primary_key = 'Unit' WHERE copy_system_id = ? AND system_id = ?"
  def setUserPrimaryKey(columnInfo: ColumnInfo[CT]) =
    using(conn.prepareStatement(setUserPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      val count = t("set-user-primary-key", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Column did not exist to have it set as primary key?")
      columnInfo.copy(isUserPrimaryKey = true)
    }

  def clearUserPrimaryKeyQuery = "UPDATE column_map SET is_user_primary_key = NULL WHERE copy_system_id = ? and system_id = ?"
  def clearUserPrimaryKey(columnInfo: ColumnInfo[CT]) = {
    require(columnInfo.isUserPrimaryKey, "Requested clearing a non-primary key")
    using(conn.prepareStatement(clearUserPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      stmt.executeUpdate()
    }
    columnInfo.copy(isUserPrimaryKey = false)
  }

  def updateNextCounterValueQuery = "UPDATE dataset_map SET next_counter_value = ? WHERE system_id = ?"
  def updateNextCounterValue(datasetInfo: DatasetInfo, newNextCounterValue: Long): DatasetInfo = {
    assert(newNextCounterValue >= datasetInfo.nextCounterValue)
    if(newNextCounterValue != datasetInfo.nextCounterValue) {
      using(conn.prepareStatement(updateNextCounterValueQuery)) { stmt =>
        stmt.setLong(1, newNextCounterValue)
        stmt.setDatasetId(2, datasetInfo.systemId)
        t("update-next-row-id", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
      }
      datasetInfo.copy(nextCounterValue = newNextCounterValue)
    } else {
      datasetInfo
    }
  }

  def updateNextCounterValue(copyInfo: CopyInfo, newNextCounterValue: Long): CopyInfo =
    copyInfo.copy(datasetInfo = updateNextCounterValue(copyInfo.datasetInfo, newNextCounterValue))

  def updateDataVersionQuery = "UPDATE copy_map SET data_version = ? WHERE system_id = ?"
  def updateDataAndShapeVersionQuery = "UPDATE copy_map SET data_version = ?, data_shape_version = ? WHERE system_id = ?"
  def updateDataVersion(copyInfo: CopyInfo, newDataVersion: Long, dataShapeUpdated: Boolean): CopyInfo = {
    // Not "== copyInfo.dataVersion + 1" because if a working copy was dropped
    assert(newDataVersion > copyInfo.dataVersion, s"Setting data version to $newDataVersion when it was ${copyInfo.dataVersion}")
    if(dataShapeUpdated) {
      using(conn.prepareStatement(updateDataAndShapeVersionQuery)) { stmt =>
        stmt.setLong(1, newDataVersion)
        stmt.setLong(2, newDataVersion)
        stmt.setLong(3, copyInfo.systemId.underlying)
        val count = t("update-data-version", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber)(stmt.executeUpdate())
        assert(count == 1)
      }
      copyInfo.copy(dataVersion = newDataVersion, dataShapeVersion = newDataVersion)
    } else {
      using(conn.prepareStatement(updateDataVersionQuery)) { stmt =>
        stmt.setLong(1, newDataVersion)
        stmt.setLong(2, copyInfo.systemId.underlying)
        val count = t("update-data-version", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber)(stmt.executeUpdate())
        assert(count == 1)
      }
      copyInfo.copy(dataVersion = newDataVersion)
    }
  }

  def updateLastModifiedQuery = "UPDATE copy_map SET last_modified = ? WHERE system_id = ?"
  def updateLastModified(copyInfo: CopyInfo, newLastModified: DateTime = currentTime()): CopyInfo = {
    using(conn.prepareStatement(updateLastModifiedQuery)) { stmt =>
      stmt.setTimestamp(1, toTimestamp(newLastModified))
      stmt.setLong(2, copyInfo.systemId.underlying)
      val count = t("update-last-modified", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber)(stmt.executeUpdate())
      assert(count == 1)
    }
    copyInfo.copy(lastModified = newLastModified)
  }

  // would prefer to use postgres upsert for this, but we're not on a new enough pg everywhere yet
  val findTableModifierQuery = "SELECT table_modifier FROM copy_map_table_modifiers WHERE copy_system_id = ? FOR UPDATE"
  val updateTableModifierQuery = "UPDATE copy_map_table_modifiers SET table_modifier = ? WHERE copy_system_id = ?"
  val createTableModifierQuery = "INSERT INTO copy_map_table_modifiers (copy_system_id, table_modifier) VALUES (?, ?)"
  def newTableModifier(copyInfo: CopyInfo): CopyInfo = {
    val oldTableModifier =
      using(conn.prepareStatement(findTableModifierQuery)) { stmt =>
        stmt.setLong(1, copyInfo.systemId.underlying)
        using(stmt.executeQuery()) { rs =>
          if(rs.next()) Some(rs.getLong("table_modifier"))
          else None
        }
      }
    oldTableModifier match {
      case Some(oldTM) =>
        val newTM = oldTM + 1
        using(conn.prepareStatement(updateTableModifierQuery)) { stmt =>
          stmt.setLong(1, newTM)
          stmt.setLong(2, copyInfo.systemId.underlying)
          stmt.executeUpdate()
        }
        copyInfo.copy(tableModifier = Some(newTM))
      case None =>
        using(conn.prepareStatement(createTableModifierQuery)) { stmt =>
          stmt.setLong(1, copyInfo.systemId.underlying)
          stmt.setLong(2, 1)
          stmt.executeUpdate()
        }
        copyInfo.copy(tableModifier = Some(1))
    }
  }

  def dropCopyQuery = "UPDATE copy_map SET lifecycle_stage = 'Discarded' WHERE system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)"
  def dropCopy(copyInfo: CopyInfo) {
    val validStages = Set(LifecycleStage.Snapshotted, LifecycleStage.Unpublished)
    if(!validStages(copyInfo.lifecycleStage)) {
      throw new CopyInWrongStateForDropException(copyInfo, validStages)
    }
    if(copyInfo.lifecycleStage == LifecycleStage.Unpublished && copyInfo.copyNumber == 1) {
      throw new CannotDropInitialWorkingCopyException(copyInfo)
    }

    dropRollup(copyInfo, None)
    using(conn.prepareStatement(dropCopyQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.setString(2, copyInfo.lifecycleStage.name) // just to make sure the user wasn't mistaken about the stage
      val count = t("drop-copy", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber)(stmt.executeUpdate())
      assert(count == 1, "Copy did not exist to be dropped?")
    }
  }

  def ensureUnpublishedCopyQuery_newCopyNumber = "SELECT max(copy_number) + 1 FROM copy_map WHERE dataset_system_id = ?"
  def ensureUnpublishedCopyQuery_copyMap = "INSERT INTO copy_map (dataset_system_id, copy_number, lifecycle_stage, data_version, data_shape_version, last_modified) values (?, ?, CAST(? AS dataset_lifecycle_stage), ?, ?, ?) RETURNING system_id"
  def ensureUnpublishedCopy(tableInfo: DatasetInfo): Either[CopyInfo, CopyPair[CopyInfo]] =
    ensureUnpublishedCopy(tableInfo, None)

  def createUnpublishedCopyWithId(tableInfo: DatasetInfo, copyId: CopyId): CopyPair[CopyInfo] =
    ensureUnpublishedCopy(tableInfo, Some(copyId)).right.getOrElse {
      throw new CopySystemIdAlreadyInUse(copyId)
    }

  def ensureUnpublishedCopy(tableInfo: DatasetInfo, newCopyId: Option[CopyId]): Either[CopyInfo, CopyPair[CopyInfo]] =
    lookup(tableInfo, LifecycleStage.Unpublished) match {
      case Some(unpublished) =>
        Left(unpublished)
      case None =>
        lookup(tableInfo, LifecycleStage.Published) match {
          case Some(publishedCopy) =>
            val newCopyNumber = using(conn.prepareStatement(ensureUnpublishedCopyQuery_newCopyNumber)) { stmt =>
              stmt.setDatasetId(1, publishedCopy.datasetInfo.systemId)
              using(t("find-next-copy-number","dataset_id" -> tableInfo.systemId)(stmt.executeQuery())) { rs =>
                rs.next()
                rs.getLong(1)
              }
            }

            val newCopy = newCopyId match {
              case None =>
                val newCopyWithoutSystemId = publishedCopy.copy(
                  systemId = new CopyId(-1),
                  copyNumber = newCopyNumber,
                  lifecycleStage = LifecycleStage.Unpublished,
                  tableModifier = None)

                val newCopy = using(conn.prepareStatement(ensureUnpublishedCopyQuery_copyMap)) { stmt =>
                  stmt.setDatasetId(1, newCopyWithoutSystemId.datasetInfo.systemId)
                  stmt.setLong(2, newCopyWithoutSystemId.copyNumber)
                  stmt.setString(3, newCopyWithoutSystemId.lifecycleStage.name)
                  stmt.setLong(4, newCopyWithoutSystemId.dataVersion)
                  stmt.setLong(5, newCopyWithoutSystemId.dataShapeVersion)
                  stmt.setTimestamp(6, toTimestamp(newCopyWithoutSystemId.lastModified))
                  using(t("create-new-copy", "dataset_id" -> newCopyWithoutSystemId.datasetInfo.systemId)(stmt.executeQuery())) { rs =>
                    val foundSomething = rs.next()
                    assert(foundSomething, "Insert didn't create a row?")
                    newCopyWithoutSystemId.copy(systemId = new CopyId(rs.getLong(1)))
                  }
                }

                copySchemaIntoUnpublishedCopy(publishedCopy, newCopy)
                copyRollupsIntoUnpublishedCopy(publishedCopy, newCopy)
                copyIndexesIntoUnpublishedCopy(publishedCopy, newCopy)

                newCopy
              case Some(cid) =>
                unsafeCreateCopy(
                  publishedCopy.datasetInfo,
                  cid,
                  newCopyNumber,
                  LifecycleStage.Unpublished,
                  publishedCopy.dataVersion,
                  publishedCopy.dataShapeVersion)
            }

            Right(CopyPair(publishedCopy, newCopy))
          case None =>
            sys.error("No published copy available?")
        }
    }

  def ensureUnpublishedCopyQuery_columnMap =
    "INSERT INTO column_map (copy_system_id, system_id, user_column_id, field_name, field_name_casefolded, type_name, physical_column_base_base, is_system_primary_key, is_user_primary_key, is_version) " +
      "SELECT ?, system_id, user_column_id, field_name, field_name_casefolded, type_name, physical_column_base_base, null, null, null " +
      "FROM column_map WHERE copy_system_id = ?;" +
    "INSERT INTO computation_strategy_map (copy_system_id, column_system_id, strategy_type, source_column_ids, parameters) " +
      "SELECT ?, column_system_id, strategy_type, source_column_ids, parameters FROM computation_strategy_map " +
      "WHERE copy_system_id = ?;" +
    "INSERT INTO index_directive_map (copy_system_id, column_system_id, directive, created_at, updated_at) " +
      "SELECT ?, column_system_id, directive, created_at, updated_at FROM index_directive_map " +
      "WHERE copy_system_id = ? AND deleted_at is null"
  def copySchemaIntoUnpublishedCopy(oldCopy: CopyInfo, newCopy: CopyInfo) {
    using(conn.prepareStatement(ensureUnpublishedCopyQuery_columnMap)) { stmt =>
      stmt.setLong(1, newCopy.systemId.underlying)
      stmt.setLong(2, oldCopy.systemId.underlying)
      stmt.setLong(3, newCopy.systemId.underlying)
      stmt.setLong(4, oldCopy.systemId.underlying)
      stmt.setLong(5, newCopy.systemId.underlying)
      stmt.setLong(6, oldCopy.systemId.underlying)
      t("copy-schema-to-unpublished-copy", "dataset_id" -> oldCopy.datasetInfo.systemId, "old_copy_num" -> oldCopy.copyNumber, "new_copy_num" -> newCopy.copyNumber)(stmt.execute())
    }
  }

  def ensureUnpublishedCopyQuery_rollupMap =
    "INSERT INTO rollup_map (name, copy_system_id, soql, raw_soql) SELECT name, ?, soql, raw_soql FROM rollup_map WHERE copy_system_id = ?"
  def copyRollupsIntoUnpublishedCopy(oldCopy: CopyInfo, newCopy: CopyInfo) {
    using(conn.prepareStatement(ensureUnpublishedCopyQuery_rollupMap)) { stmt =>
      stmt.setLong(1, newCopy.systemId.underlying)
      stmt.setLong(2, oldCopy.systemId.underlying)
      t("copy-rollups-to-unpublished-copy", "dataset_id" -> oldCopy.datasetInfo.systemId, "old_copy_num" -> oldCopy.copyNumber, "new_copy_num" -> newCopy.copyNumber)(stmt.execute())
    }
  }

  def ensureUnpublishedCopyQuery_indexMap =
    "INSERT INTO index_map (name, copy_system_id, expressions, filter, created_at, updated_at) SELECT name, ?, expressions, filter, created_at, updated_at FROM index_map WHERE copy_system_id = ? AND deleted_at is null"
  def copyIndexesIntoUnpublishedCopy(oldCopy: CopyInfo, newCopy: CopyInfo) {
    using(conn.prepareStatement(ensureUnpublishedCopyQuery_indexMap)) { stmt =>
      stmt.setLong(1, newCopy.systemId.underlying)
      stmt.setLong(2, oldCopy.systemId.underlying)
      t("copy-indexes-to-unpublished-copy", "dataset_id" -> oldCopy.datasetInfo.systemId, "old_copy_num" -> oldCopy.copyNumber, "new_copy_num" -> newCopy.copyNumber)(stmt.execute())
    }
  }

  def publishQuery = "UPDATE copy_map SET lifecycle_stage = CAST(? AS dataset_lifecycle_stage) WHERE system_id = ?"
  def publish(unpublishedCopy: CopyInfo): (CopyInfo, Option[CopyInfo]) = {
    if(unpublishedCopy.lifecycleStage != LifecycleStage.Unpublished) {
      throw new IllegalArgumentException("Input does not name an unpublished copy")
    }
    using(conn.prepareStatement(publishQuery)) { stmt =>
      val publishedCI = for(published <- lookup(unpublishedCopy.datasetInfo, LifecycleStage.Published)) yield {
        stmt.setString(1, LifecycleStage.Snapshotted.name)
        stmt.setLong(2, published.systemId.underlying)
        val count = t("snapshotify-published-copy", "dataset_id" -> published.datasetInfo.systemId, "copy_num" -> published.copyNumber)(stmt.executeUpdate())
        assert(count == 1, "Snapshotting a published copy didn't change a row?")
        published.copy(lifecycleStage = LifecycleStage.Snapshotted)
      }
      stmt.setString(1, LifecycleStage.Published.name)
      stmt.setLong(2, unpublishedCopy.systemId.underlying)
      val count = t("publish-unpublished-copy", "dataset_id" -> unpublishedCopy.datasetInfo.systemId, "copy_num" -> unpublishedCopy.copyNumber)(stmt.executeUpdate())
      assert(count == 1, "Publishing an unpublished copy didn't change a row?")
      (unpublishedCopy.copy(lifecycleStage = LifecycleStage.Published), publishedCI)
    }
  }

  def insertRollupQuery = "INSERT INTO rollup_map (name, copy_system_id, soql, raw_soql) VALUES (?, ?, ?, ?)"
  def updateRollupQuery = "UPDATE rollup_map SET soql = ?, raw_soql = ? WHERE name = ? AND copy_system_id = ?"
  def createOrUpdateRollup(copyInfo: CopyInfo, name: RollupName, soql: String, rawSoql: Option[String]): RollupInfo = {
    rollup(copyInfo, name) match {
      case Some(_) =>
        using(conn.prepareStatement(updateRollupQuery)) { stmt =>
          stmt.setString(1, soql)
          stmt.setString(2, rawSoql.orNull)
          stmt.setString(3, name.underlying)
          stmt.setLong(4, copyInfo.systemId.underlying)
          t("create-or-update-rollup", "action" -> "update", "copy-id" -> copyInfo.systemId, "name" -> name)(stmt.executeUpdate())
        }
      case None =>
        using(conn.prepareStatement(insertRollupQuery)) { stmt =>
          stmt.setString(1, name.underlying)
          stmt.setLong(2, copyInfo.systemId.underlying)
          stmt.setString(3, soql)
          stmt.setString(4, rawSoql.orNull)
          t("create-or-update-rollup", "action" -> "insert", "copy-id" -> copyInfo.systemId, "name" -> name)(stmt.executeUpdate())
        }
    }
    RollupInfo(copyInfo, name, soql, rawSoql)
  }

  private val DropRollupsQuery = "DELETE FROM rollup_map WHERE copy_system_id = ?"
  private val DropRollupQuery = s"$DropRollupsQuery AND name = ?"

  /**
   * @param copyInfo - Copy of rollups that are to be dropped.
   * @param rollupName - If Some, drop one rollup.  If None, drop all rollups.
   */
  def dropRollup(copyInfo: CopyInfo, rollupName: Option[RollupName]) {
    rollupName match {
      case Some(name) =>
          using(conn.prepareStatement(DropRollupQuery)) { stmt =>
            stmt.setLong(1, copyInfo.systemId.underlying)
            stmt.setString(2, name.underlying)
            t("drop-rollup", "copy-id" -> copyInfo.systemId, "name" -> name)(stmt.executeUpdate())
          }
      case None =>
        using(conn.prepareStatement(DropRollupsQuery)) { stmt =>
          stmt.setLong(1, copyInfo.systemId.underlying)
          t("drop-rollup", "copy-id" -> copyInfo.systemId)(stmt.executeUpdate())
        }
    }
  }

  def insertIndexQuery = "INSERT INTO index_map (name, copy_system_id, expressions, filter) VALUES (?, ?, ?, ?) RETURNING system_id"
  def updateIndexQuery = "UPDATE index_map SET expressions = ?, filter = ?, updated_at = now() WHERE name = ? AND copy_system_id = ?"
  def createOrUpdateIndex(copyInfo: CopyInfo, name: IndexName, expressions: String, filter: Option[String]): IndexInfo = {
    val indexId = index(copyInfo, name) match {
      case Some(exist) =>
        using(conn.prepareStatement(updateIndexQuery)) { stmt =>
          stmt.setString(1, expressions)
          stmt.setString(2, filter.orNull)
          stmt.setString(3, name.underlying)
          stmt.setLong(4, copyInfo.systemId.underlying)
          t("create-or-update-index", "action" -> "update", "copy-id" -> copyInfo.systemId, "name" -> name)(stmt.executeUpdate())
          exist.systemId
        }
      case None =>
        using(conn.prepareStatement(insertIndexQuery)) { stmt =>
          stmt.setString(1, name.underlying)
          stmt.setLong(2, copyInfo.systemId.underlying)
          stmt.setString(3, expressions)
          stmt.setString(4, filter.orNull)
          t("create-or-update-index", "action" -> "insert", "copy-id" -> copyInfo.systemId, "name" -> name) {
            using(stmt.executeQuery()) { rs =>
              if (rs.next()) rs.getIndexId(1)
              else throw new FailToCreateIndex(copyInfo, name)
            }
          }
        }
    }
    IndexInfo(indexId, copyInfo, name, expressions, filter)
  }

  private val DropIndexsQuery = "DELETE FROM index_map WHERE copy_system_id = ?"
  private val DropIndexQuery = s"$DropIndexsQuery AND name = ?"

  /**
    * @param copyInfo - Copy of Indexs that are to be dropped.
    * @param IndexName - If Some, drop one Index.  If None, drop all Indexs.
    */
  def dropIndex(copyInfo: CopyInfo, IndexName: Option[IndexName]) {
    IndexName match {
      case Some(name) =>
        using(conn.prepareStatement(DropIndexQuery)) { stmt =>
          stmt.setLong(1, copyInfo.systemId.underlying)
          stmt.setString(2, name.underlying)
          t("drop-index", "copy-id" -> copyInfo.systemId, "name" -> name)(stmt.executeUpdate())
        }
      case None =>
        using(conn.prepareStatement(DropIndexsQuery)) { stmt =>
          stmt.setLong(1, copyInfo.systemId.underlying)
          t("drop-indexes", "copy-id" -> copyInfo.systemId)(stmt.executeUpdate())
        }
    }
  }

  def lockNotAvailableState = "55P03"
  def queryCancelledState = "57014"
  def readOnlySqlTransactionState = "25006"

  def isStatementTimeout(e: PSQLException): Boolean =
    e.getSQLState == queryCancelledState &&
      errorMessage(e).map(_.endsWith("due to statement timeout")).getOrElse(false)

  def isReadOnlyTransaction(e: PSQLException): Boolean =
    e.getSQLState == readOnlySqlTransactionState

  def errorMessage(e: PSQLException): Option[String] =
    for {
      serverErrorMessage <- Option(e.getServerErrorMessage)
      message <- Option(serverErrorMessage.getMessage)
    } yield message
}

object BasePostgresDatasetMapWriter {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[BasePostgresDatasetMapWriter[_]])
}

class PostgresDatasetMapWriter[CT](val conn: Connection, tns: TypeNamespace[CT], timingReport: TimingReport, val obfuscationKeyGenerator: () => Array[Byte], val initialCounterValue: Long, val initialLatestDataVersion: Long) extends DatasetMapWriter[CT] with BasePostgresDatasetMapWriter[CT] with BackupDatasetMap[CT] {
  implicit def typeNamespace = tns
  require(!conn.getAutoCommit, "Connection is in auto-commit mode")

  import PostgresDatasetMapWriter._

  def t = timingReport

  // Can't set parameters' values via prepared statement placeholders
  def setTimeout(timeoutMs: Int) =
    s"SET LOCAL statement_timeout TO $timeoutMs"
  def datasetInfoBySystemIdQuery(semiExclusive: Boolean) =
    "SELECT system_id, next_counter_value, latest_data_version, locale_name, obfuscation_key, resource_name FROM dataset_map WHERE system_id = ? FOR %s".format(
      if(semiExclusive) "SHARE" else "UPDATE"
    )
  def resetTimeout = "SET LOCAL statement_timeout TO DEFAULT"
  def datasetInfo(datasetId: DatasetId, timeout: Duration, semiExclusive: Boolean): Option[DatasetInfo] = {
    // For now we assume that we're the only one setting the statement_timeout
    // parameter.  If this turns out to be wrong, we'll have to SHOW the
    // parameter in order to
    //   * set the value properly in the case of a nonfinite timeout
    //   * save the value to restore.
    // One might think this would be better done as a stored procedure,
    // which can do the save/restore thing automatically -- see the paragraph
    // that begins "If SET LOCAL is used within a function..." at
    //     http://www.postgresql.org/docs/9.2/static/sql-set.html
    // but unfortunately setting statement_timeout doesn't affect the
    // _current_ statement.  For this same reason we're not just doing all
    // three operations in a single "set timeout;query;restore timeout"
    // call.

    val savepoint = conn.setSavepoint()
    try {
      if(timeout.isFinite()) {
        val ms = timeout.toMillis.min(Int.MaxValue).max(1).toInt
        log.trace("Setting statement timeout to {}ms", ms)
        execute(setTimeout(ms))
      }
      val result =
        using(conn.prepareStatement(datasetInfoBySystemIdQuery(semiExclusive))) { stmt =>
          stmt.setDatasetId(1, datasetId)
          try {
            t("lookup-dataset-for-update", "dataset_id" -> datasetId)(stmt.execute())
            getInfoResult(stmt)
          } catch {
            case e: PSQLException if isStatementTimeout(e) =>
              log.trace("Get dataset _with_ waiting failed; abandoning")
              conn.rollback(savepoint)
              throw new DatasetIdInUseByWriterException(datasetId, e)
            case e: PSQLException if isReadOnlyTransaction(e) =>
              log.trace("Get dataset for update failed due to read-only txn; abandoning")
              conn.rollback(savepoint)
              throw new DatabaseInReadOnlyMode(e)
          }
        }
      if(timeout.isFinite) execute(resetTimeout)
      result
    } finally {
      try {
        conn.releaseSavepoint(savepoint)
      } catch {
        case e: SQLException =>
          // Ignore; this means one of two things:
          // * the server is in an unexpected "transaction aborted" state, so all we
          //    can do is roll back (either to another, earlier savepoint or completely)
          //    and either way this savepoint will be dropped implicitly
          // * things have completely exploded and nothing can be done except
          //    dropping the connection altogether.
          // The latter could happen if this finally block is being run because
          // this method is exiting normally, but in that case whatever we do next
          // will fail so meh.  Just log it and continue.
          log.warn("Unexpected exception while releasing savepoint", e)
      }
    }
  }

  def execute(s: String) {
    using(conn.createStatement()) { stmt =>
      log.trace("Executing simple SQL {}", s)
      stmt.execute(s)
    }
  }

  def getInfoResult(stmt: Statement) =
    using(stmt.getResultSet) { rs =>
      if(rs.next()) {
        Some(DatasetInfo(rs.getDatasetId("system_id"), rs.getLong("next_counter_value"), rs.getLong("latest_data_version"), rs.getString("locale_name"), rs.getBytes("obfuscation_key"), Option(rs.getString("resource_name"))))
      } else {
        None
      }
    }
}

object PostgresDatasetMapWriter {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresDatasetMapWriter[_]])
}
