package com.socrata.datacoordinator.primary

class Publisher[CT, CV](mutator: DatabaseMutator[CT, CV]) {
  def publish(dataset: String, username: String) {
    mutator.withSchemaUpdate(dataset, username) { su =>
      import su._
      datasetMap.publish(initialCopyInfo)
      datasetLog.workingCopyPublished()
    }
  }
}
