package com.socrata.datacoordinator.secondary

import java.util.UUID

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import com.socrata.datacoordinator.id.DatasetId

@JsonKeyStrategy(Strategy.Underscore)
case class SecondaryMoveJob(id: UUID,
                            datasetId: DatasetId,
                            fromStoreId: String,
                            toStoreId: String,
                            moveFromStoreComplete: Boolean,
                            moveToStoreComplete: Boolean,
                            createdAtMillis: Long)

object SecondaryMoveJob {
  implicit val codec = AutomaticJsonCodecBuilder[SecondaryMoveJob]

  implicit object CreatedAtOrdering extends Ordering[SecondaryMoveJob] {
    override def compare(x: SecondaryMoveJob, y: SecondaryMoveJob): Int = x.createdAtMillis.compare(y.createdAtMillis)
  }
}

trait SecondaryMoveJobs {
  def jobs(jobId: UUID): Seq[SecondaryMoveJob]
  def jobs(datasetId: DatasetId, includeCompleted: Boolean = false): Seq[SecondaryMoveJob]
  def jobsFromStore(storeId: String, datasetId: DatasetId, includeCompleted: Boolean = false): Seq[SecondaryMoveJob]
  def jobsToStore(storeId: String, datasetId: DatasetId, includeCompleted: Boolean = false): Seq[SecondaryMoveJob]
  def addJob(jobId: UUID, datasetId: DatasetId, fromStoreId: String, toStoreId: String): Unit
  def markJobsFromStoreComplete(storeId: String, datasetId: DatasetId): Unit
  def markJobsToStoreComplete(storeId: String, datasetId: DatasetId): Unit
}