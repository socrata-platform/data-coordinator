package com.socrata.datacoordinator.secondary

trait CollocationManifest {
  def collocatedDatasets(datasets: Set[String]): Set[String]
  def addCollocations(collocations: Set[(String, String)]): Unit
  def dropCollocations(dataset: String): Unit
}
