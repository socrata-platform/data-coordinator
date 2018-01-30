package com.socrata.datacoordinator.collocation

import com.socrata.datacoordinator.id.DatasetInternalName

class CollocationManifest {
  private val manifest = collection.mutable.Set.empty[(DatasetInternalName, DatasetInternalName)]

  def add(collocations: Seq[(DatasetInternalName, DatasetInternalName)]): Unit = {
    collocations.foreach(collocation => manifest.add(collocation))
  }

  def get: Set[(DatasetInternalName, DatasetInternalName)] = manifest.toSet
}
