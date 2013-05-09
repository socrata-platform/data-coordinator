package com.socrata.datacoordinator.truth.backuplog

import com.socrata.datacoordinator.id.DatasetId

case class BackupRecord(datasetId: DatasetId, startingDataVersion: Long, endingDataVersion: Long)

trait BackupLog {
  def findDatasetsNeedingBackup(limit: Int = 1000): Seq[BackupRecord]
  def completedBackupTo(datasetId: DatasetId, dataVersion: Long)
}
