package com.socrata.datacoordinator.truth.metadata

import org.joda.time.DateTime
import com.socrata.datacoordinator.id.{GlobalLogEntryId, DatasetId}
import com.socrata.datacoordinator.util.CloseableIterator

trait GlobalLog {
  /** Notes that a dataset has changed in some way.  The actual change log is in the dataset's private log table.
    * @note This may take an exclusive write-lock on a shared table.  THIS SHOULD BE THE VERY LAST THING
    *       A TRANSACTION DOES BEFORE IT COMMITS IN ORDER TO MINIMIZE THAT LOCK-TIME. */
  def log(datasetInfo: DatasetInfo, version: Long, updatedAt: DateTime, updatedBy: String)
}

trait GlobalLogPlayback {
  case class Job(id: GlobalLogEntryId, datasetId: DatasetId, version: Long)
  def pendingJobs(aboveJob: GlobalLogEntryId): Iterator[Job]
}

trait BackupManifest {
  def lastJob(): GlobalLogEntryId
  def finishedJob(job: GlobalLogEntryId)
}

