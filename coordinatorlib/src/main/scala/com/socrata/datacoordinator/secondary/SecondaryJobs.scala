package com.socrata.datacoordinator.secondary

import java.util.UUID

import com.socrata.datacoordinator.id.DatasetId

case class SecondaryJob(id: UUID, step: Short, storeId: String, datasetId: DatasetId)

trait SecondaryJobs {
  def job(jobId: UUID, step: Short): Option[SecondaryJob]
  def jobs(storeId: String, datasetId: DatasetId): Seq[SecondaryJob]
  def addJob(storeId: String, datasetId: DatasetId, jobId: UUID, step: Short): Unit
  def deleteJobs(storeId: String, datasetId: DatasetId): Unit
  def deleteJobs(jobId: UUID, step: Short): Unit
}