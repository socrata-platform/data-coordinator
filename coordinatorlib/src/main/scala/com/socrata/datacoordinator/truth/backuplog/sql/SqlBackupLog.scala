package com.socrata.datacoordinator.truth.backuplog
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.id.sql._
import scala.collection.immutable.VectorBuilder

class SqlBackupLog(conn: Connection) extends BackupLog {
  def findDatasetsNeedingBackup(limit: Int): Seq[BackupRecord] =
    using(conn.prepareStatement("SELECT dataset_system_id, latest_backup_data_version, latest_data_version FROM backup_log WHERE latest_data_version > latest_backup_data_version ORDER BY went_out_of_sync_at LIMIT ?")) { stmt =>
      stmt.setInt(1, limit)
      using(stmt.executeQuery()) { rs =>
        val results = new VectorBuilder[BackupRecord]
        while(rs.next()) {
          results += BackupRecord(
            rs.getDatasetId("dataset_system_id"),
            startingDataVersion = rs.getLong("latest_backup_data_version") + 1,
            endingDataVersion = rs.getLong("latest_data_version")
          )
        }
        results.result()
      }
    }

  def completedBackupTo(datasetId: DatasetId, dataVersion: Long) {
    using(conn.prepareStatement("UPDATE backup_log SET latest_backup_data_version = ?, went_out_of_sync_at = CURRENT_TIMESTAMP WHERE dataset_system_id = ?")) { stmt =>
      stmt.setLong(1, dataVersion)
      stmt.setDatasetId(2, datasetId)
      stmt.executeUpdate()
    }
  }
}
