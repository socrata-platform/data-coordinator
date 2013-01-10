package com.socrata.datacoordinator
package truth.metadata
package sql
package `-impl`

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata.`-impl`.DatasetMapReaderAPI
import com.socrata.datacoordinator.id.{ColumnId, VersionId, DatasetId}
import com.socrata.datacoordinator.util.collection.MutableColumnIdMap

/** Implementation of [[com.socrata.datacoordinator.truth.metadata.`-impl`.DatasetMapReaderAPI]]
  * for Postgresql. */
abstract class PostgresDatasetMapReaderAPI(val conn: Connection) extends DatasetMapReaderAPI {
  type DatasetInfo = SqlDatasetInfo
  type VersionInfo = SqlVersionInfo
  type ColumnInfo = SqlColumnInfo

  case class SqlDatasetInfo(systemId: DatasetId, datasetId: String, tableBase: String) extends IDatasetInfo
  case class SqlVersionInfo(datasetInfo: SqlDatasetInfo, systemId: VersionId, lifecycleVersion: Long, lifecycleStage: LifecycleStage) extends IVersionInfo
  case class SqlColumnInfo(versionInfo: SqlVersionInfo, systemId: ColumnId, logicalName: String, typeName: String, physicalColumnBaseBase: String, isUserPrimaryKey: Boolean) extends IColumnInfo

  require(!conn.getAutoCommit, "Connection is in auto-commit mode")

  def snapshotCountQuery = "SELECT count(system_id) FROM version_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)"
  def snapshotCount(dataset: DatasetInfo) =
    using(conn.prepareStatement(snapshotCountQuery)) { stmt =>
      stmt.setLong(1, dataset.systemId.underlying)
      stmt.setString(2, LifecycleStage.Snapshotted.name)
      using(stmt.executeQuery()) { rs =>
        rs.next()
        rs.getInt(1)
      }
    }

  def latestQuery = "SELECT system_id, lifecycle_version, lifecycle_stage :: TEXT FROM version_map WHERE dataset_system_id = ? ORDER BY lifecycle_version DESC LIMIT 1"
  def latest(datasetInfo: DatasetInfo) =
    using(conn.prepareStatement(latestQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      using(stmt.executeQuery()) { rs =>
        if(!rs.next()) sys.error("Looked up a table for " + datasetInfo.datasetId + " but didn't find any version info?")
        SqlVersionInfo(datasetInfo, new VersionId(rs.getLong("system_id")), rs.getLong("lifecycle_version"), LifecycleStage.valueOf(rs.getString("lifecycle_stage")))
      }
    }

  def lookupQuery = "SELECT system_id, lifecycle_version FROM version_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage) ORDER BY lifecycle_version DESC OFFSET ? LIMIT 1"
  def lookup(datasetInfo: DatasetInfo, stage: LifecycleStage, nth: Int = 0) = {
    using(conn.prepareStatement(lookupQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      stmt.setString(2, stage.name)
      stmt.setInt(3, nth)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(SqlVersionInfo(datasetInfo, new VersionId(rs.getLong("system_id")), rs.getLong("lifecycle_version"), stage))
        } else {
          None
        }
      }
    }
  }

  def schemaQuery = "SELECT system_id, logical_column, type_name, physical_column_base_base, (is_user_primary_key IS NOT NULL) is_user_primary_key FROM column_map WHERE version_system_id = ?"
  def schema(versionInfo: VersionInfo) = {
    using(conn.prepareStatement(schemaQuery)) { stmt =>
      stmt.setLong(1, versionInfo.systemId.underlying)
      using(stmt.executeQuery()) { rs =>
        val result = new MutableColumnIdMap[ColumnInfo]
        while(rs.next()) {
          val systemId = new ColumnId(rs.getLong("system_id"))
          result += systemId -> SqlColumnInfo(versionInfo, systemId, rs.getString("logical_column"), rs.getString("type_name"), rs.getString("physical_column_base_base"), rs.getBoolean("is_user_primary_key"))
        }
        result.freeze()
      }
    }
  }

  // These are from the reader trait but they're used in the writer tests
  def unpublished(datasetInfo: DatasetInfo) =
    lookup(datasetInfo, LifecycleStage.Unpublished)

  def published(datasetInfo: DatasetInfo) =
    lookup(datasetInfo, LifecycleStage.Published)

  def snapshot(datasetInfo: DatasetInfo, age: Int) =
    lookup(datasetInfo, LifecycleStage.Snapshotted, age)
}
