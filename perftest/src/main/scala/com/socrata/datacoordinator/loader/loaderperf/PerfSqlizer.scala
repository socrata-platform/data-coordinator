package com.socrata.datacoordinator.loader
package loaderperf

class PerfSqlizer(datasetContext: DatasetContext[PerfType, PerfValue]) extends Sqlizer {
  def logTransactionComplete() {
  }

  def lockTableAgainstWrites(table: String) = {
    "LOCK TABLE " + table + " IN EXCLUSIVE MODE NOWAIT"
  }
}
