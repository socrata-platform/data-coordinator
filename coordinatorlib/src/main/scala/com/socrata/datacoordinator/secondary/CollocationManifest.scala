package com.socrata.datacoordinator.secondary

trait CollocationManifest {
  def collocatedDatasets(datasets: Set[String]): Set[String]
}
