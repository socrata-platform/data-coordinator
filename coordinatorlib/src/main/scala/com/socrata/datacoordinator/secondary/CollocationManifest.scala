package com.socrata.datacoordinator.secondary

import java.util.UUID

trait CollocationManifest {
  def collocatedDatasets(datasets: Set[String]): Set[String]
  def addCollocations(jobId: UUID, collocations: Set[(String, String)]): Unit
  def dropCollocations(dataset: String): Unit
  def dropCollocations(jobId: UUID): Unit
}
