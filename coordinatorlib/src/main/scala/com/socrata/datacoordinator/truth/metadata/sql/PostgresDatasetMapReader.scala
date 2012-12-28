package com.socrata.datacoordinator
package truth.metadata
package sql

import java.sql.{PreparedStatement, Connection}

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.{VersionId, DatasetId}

class PostgresDatasetMapReader(_conn: Connection) extends `-impl`.PostgresDatasetMapReaderAPI(_conn) with DatasetMapReader {
  def datasetInfoByUserIdQuery = "SELECT system_id, dataset_id, table_base FROM dataset_map WHERE dataset_id = ?"
  def datasetInfo(datasetId: String) =
    using(conn.prepareStatement(datasetInfoByUserIdQuery)) { stmt =>
      stmt.setString(1, datasetId)
      extractDatasetInfoFromResultSet(stmt)
    }

  def extractDatasetInfoFromResultSet(stmt: PreparedStatement) =
    using(stmt.executeQuery()) { rs =>
      if(rs.next()) {
        Some(SqlDatasetInfo(new DatasetId(rs.getLong("system_id")), rs.getString("dataset_id"), rs.getString("table_base")))
      } else {
        None
      }
    }

  def datasetInfoBySystemIdQuery = "SELECT system_id, dataset_id, table_base FROM dataset_map WHERE system_id = ?"
  def datasetInfo(datasetId: DatasetId) =
    using(conn.prepareStatement(datasetInfoBySystemIdQuery)) { stmt =>
      stmt.setLong(1, datasetId.underlying)
      extractDatasetInfoFromResultSet(stmt)
    }

  val versionQuery = "SELECT system_id, lifecycle_stage FROM version_map WHERE dataset_system_id = ? AND lifecycle_version = ?"
  def version(datasetInfo: DatasetInfo, lifecycleVersion: Long) =
    using(conn.prepareStatement(versionQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      stmt.setLong(2, lifecycleVersion)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(SqlVersionInfo(datasetInfo, new VersionId(rs.getLong("system_id")), lifecycleVersion, LifecycleStage.valueOf(rs.getString("lifecycle_stage"))))
        } else {
          None
        }
      }
    }
}
