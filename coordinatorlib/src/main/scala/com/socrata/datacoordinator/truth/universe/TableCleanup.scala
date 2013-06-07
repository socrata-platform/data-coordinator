package com.socrata.datacoordinator.truth.universe

trait TableCleanup {
  def cleanupPendingDrops(): Boolean
}
