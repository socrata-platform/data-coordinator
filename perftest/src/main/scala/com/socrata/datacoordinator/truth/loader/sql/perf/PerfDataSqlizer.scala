package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import com.socrata.datacoordinator.truth.sql.RepBasedSqlDatasetContext

class PerfDataSqlizer(tableName: String, datasetContext: RepBasedSqlDatasetContext[PerfType, PerfValue])
  extends PostgresRepBasedDataSqlizer[PerfType, PerfValue](
    tableName,
    datasetContext
  )
