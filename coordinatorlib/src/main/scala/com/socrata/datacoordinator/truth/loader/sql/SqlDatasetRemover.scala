package com.socrata.datacoordinator.truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.DatasetIdInUseByWriterException
import com.socrata.datacoordinator.truth.loader.sql.SqlTableRemover.SqlTableRemover
import com.socrata.datacoordinator.truth.metadata.DatasetMapWriter

import scala.concurrent.duration.Duration

class SqlDatasetRemover[CT](conn: Connection, writeLockTimeout: Duration, datasetMap: DatasetMapWriter[CT]) extends DatasetRemover {
  def removeDataset(datasetId: DatasetId) = {
    try {
      val result = for {
        di <- datasetMap.datasetInfo(datasetId, writeLockTimeout)
      } yield {
          val fakeVersion = datasetMap.latest(di).dataVersion + 1

          using(new SqlTableRemover(conn)) { td =>
            for(copy <- datasetMap.allCopies(di)) {
              //TODO: Ask if the system_id is the same for all dataset copies
              td.delete(copy.dataTableName)
            }
            td.delete(di.logTableName)
            td.delete(di.auditTableName)
            td.go()
          }

          updateSecondaryAndBackupInfo(datasetId, fakeVersion)

          datasetMap.delete(di)

          DatasetRemover.Success
        }

      result.getOrElse(DatasetRemover.FailureNotFound)
    } catch {
      case _: DatasetIdInUseByWriterException =>
        DatasetRemover.FailureWriteLock
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