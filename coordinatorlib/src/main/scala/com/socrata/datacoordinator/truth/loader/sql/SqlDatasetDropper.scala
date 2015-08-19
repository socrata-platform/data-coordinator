package com.socrata.datacoordinator.truth.loader
package sql

import com.socrata.datacoordinator.id.DatasetId
import java.sql.Connection
import scala.concurrent.duration.Duration
import com.socrata.datacoordinator.truth.metadata.DatasetMapWriter
import com.rojoma.simplearm.util._
import java.io.Closeable
import com.socrata.datacoordinator.truth.DatasetIdInUseByWriterException

//This method was used in a previous implementation of table dropper, where data coordinator put tables first in pending_table_drops
//Then at a later date, deletes all the datasets in pending_table_drops
class SqlDatasetDropper[CT](conn: Connection, writeLockTimeout: Duration, datasetMap: DatasetMapWriter[CT]) extends DatasetDropper {
  def dropDataset(datasetId: DatasetId) = {
    try {
      val result = for {
        di <- datasetMap.datasetInfo(datasetId, writeLockTimeout)
      } yield {
        val fakeVersion = datasetMap.latest(di).dataVersion + 1

        using(new SqlTableDropper(conn)) { td =>
          for(copy <- datasetMap.allCopies(di)) {
            td.delete(copy.dataTableName)
          }
          td.delete(di.logTableName)
          td.delete(di.auditTableName)
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

  protected def updateSecondaryAndBackupInfo(datasetId: DatasetId, fakeVersion: Long) {
    using(conn.createStatement()) { stmt =>
      stmt.executeUpdate("UPDATE secondary_manifest SET latest_data_version = " + fakeVersion +
        ", latest_secondary_lifecycle_stage = 'Discarded'::dataset_lifecycle_stage WHERE dataset_system_id = " + datasetId.underlying)
      stmt.executeUpdate("UPDATE backup_log SET latest_data_version = " + fakeVersion + " WHERE dataset_system_id = " + datasetId.underlying)
    }
  }

}
