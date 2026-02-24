package com.socrata.datacoordinator.truth.loader

trait TableDropper {
  def scheduleForDropping(tableName: String)
}
