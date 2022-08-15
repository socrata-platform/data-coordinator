package com.socrata.datacoordinator.truth.loader
package sql

import com.socrata.datacoordinator.id.DatasetId
import java.sql.Connection
import scala.concurrent.duration.Duration
import com.socrata.datacoordinator.truth.metadata.DatasetMapWriter
import com.rojoma.simplearm.v2._
import java.io.Closeable
import com.socrata.datacoordinator.truth.DatasetIdInUseByWriterException

class SqlDatasetDropper[CT](conn: Connection, writeLockTimeout: Duration, datasetMap: DatasetMapWriter[CT]) extends DatasetDropper {
  def dropDataset(datasetId: DatasetId) = {
    try {
      val result = for {
        di <- datasetMap.datasetInfo(datasetId, writeLockTimeout)
      } yield {
        val fakeVersion = datasetMap.latest(di).dataVersion + 1

        using(new SqlTableDropper(conn)) { td =>
          for(copy <- datasetMap.allCopies(di)) {
            td.scheduleForDropping(copy.dataTableName)
          }
          td.scheduleForDropping(di.logTableName)
          td.scheduleForDropping(di.auditTableName)
          td.go()
        }

        updateSecondaryAndBackupInfo(datasetId, fakeVersion)
        datasetMap.delete(di)

        DatasetDropper.Success
      }

      result.getOrElse(DatasetDropper.FailureNotFound)
    } catch {
      case _: DatasetIdInUseByWriterException =>
        DatasetDropper.FailureWriteLock
    }
  }

  /**
    * went_out_of_sync is set one day ahead of now so that the secondary_manifest record will not be prematurely
    * deleted by SqlTableCleanup before it is processed by secondary watcher.
    */
  protected def updateSecondaryAndBackupInfo(datasetId: DatasetId, fakeVersion: Long) {
    // EN-12729 Note: Ordering by store_id to avoid deadlocks here should not be necessary,
    // but we are going to do it just in case and for consistency.
    using(conn.prepareStatement(
      s"""UPDATE secondary_manifest
         |SET latest_data_version = ?,
         |    went_out_of_sync_at = now() + interval '3 days'
         |WHERE (dataset_system_id, store_id) IN (
         |  SELECT dataset_system_id, store_id FROM secondary_manifest
         |  WHERE dataset_system_id = ? ORDER BY store_id FOR UPDATE
         |)""".stripMargin)) { stmt =>
      stmt.setLong(1, fakeVersion)
      stmt.setLong(2, datasetId.underlying)
      stmt.executeUpdate()
    }
  }

}
