package com.socrata.datacoordinator.secondary.feedback.monitor

import com.socrata.datacoordinator.secondary.feedback.DataVersion

/**
 * A StatusMonitor monitors the statuses of datasets being updated by a feedback secondary
 */
trait StatusMonitor {

  /**
   * Signifies that a batch of an update have been feedback to data-coordinator.
   * @param batchSize the maximum number of rows in a batch
   * @param batchNumber the number of the batch starting at 1
   * @note for a given dataset version the batch number may go down
   */
  def update(datasetInternalName: String, version: DataVersion, batchSize: Int, batchNumber: Int): Unit

  /**
   * Signifies that all batches of a version update has been feedback to data-coordinator.
   * @param batchCount the total number of batches
   */
  def remove(datasetInternalName: String, version: DataVersion, batchCount: Int): Unit

}
