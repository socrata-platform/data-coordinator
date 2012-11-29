package com.socrata.datacoordinator
package truth.loader
package sql
package perf

class PerfSqlizer(datasetContext: DatasetContext[PerfType, PerfValue]) extends Sqlizer {
  def logTransactionComplete() {
  }

  def lockTableAgainstWrites(table: String) = {
    "LOCK TABLE " + table + " IN EXCLUSIVE MODE NOWAIT"
  }
}
