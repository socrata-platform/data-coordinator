package com.socrata.datacoordinator.secondary.feedback.monitor

import com.socrata.datacoordinator.secondary.feedback.DataVersion

/**
 * A dummy StatusMonitor that just logs.
 */
class DummyStatusMonitor extends StatusMonitor {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[DummyStatusMonitor])

  override def update(datasetInternalName: String, version: DataVersion, batchSize: Int, batchNumber: Int): Unit = {
    log.info("Update of dataset {} to version {} has completed batch {} of size {}.",
      datasetInternalName, version.underlying.toString, batchNumber.toString, batchSize.toString)
  }

  override def remove(datasetInternalName: String, version: DataVersion, batchCount: Int): Unit = {
    log.info("Update of dataset {} to version {} has completed after {} batches.",
      datasetInternalName, version.underlying.toString, batchCount.toString)
  }
}
