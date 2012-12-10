package com.socrata.datacoordinator.truth.metadata
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

class PostgresDatasetMapReader(_conn: Connection) extends `-impl`.PostgresDatasetMapReaderAPI(_conn) with DatasetMapReader {
  def datasetInfoQuery = "SELECT system_id, dataset_id, table_base FROM dataset_map WHERE dataset_id = ?"
  def datasetInfo(datasetId: String) =
    using(conn.prepareStatement(datasetInfoQuery)) { stmt =>
      stmt.setString(1, datasetId)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(SqlDatasetInfo(rs.getLong("system_id"), rs.getString("dataset_id"), rs.getString("table_base")))
        } else {
          None
        }
      }
    }

  val versionQuery = "SELECT system_id, lifecycle_stage FROM version_map WHERE dataset_system_id = ? AND lifecycle_version = ?"
  def version(datasetInfo: DatasetInfo, lifecycleVersion: Long) =
    using(conn.prepareStatement(versionQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId)
      stmt.setLong(2, lifecycleVersion)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(SqlVersionInfo(datasetInfo, rs.getLong("system_id"), lifecycleVersion, LifecycleStage.valueOf(rs.getString("lifecycle_stage"))))
        } else {
          None
        }
      }
    }
}
