package com.socrata.datacoordinator.truth.metadata

import org.joda.time.DateTime

trait GlobalLog {
  /** Notes that a dataset has changed in some way.  The actual change log is in the dataset's private log table.
    * @note This may take an exclusive write-lock on a shared table.  THIS SHOULD BE THE VERY LAST THING
    *       A TRANSACTION DOES BEFORE IT COMMITS IN ORDER TO MINIMIZE THAT LOCK-TIME. */
  def log(datasetId: String, version: Long, updatedAt: DateTime, updatedBy: String)
}
