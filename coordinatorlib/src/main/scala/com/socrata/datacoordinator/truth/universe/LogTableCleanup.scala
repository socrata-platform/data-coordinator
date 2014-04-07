package com.socrata.datacoordinator.truth.universe

trait LogTableCleanup {
  def cleanupOldVersions(): Boolean
}
