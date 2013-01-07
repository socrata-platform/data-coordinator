package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import com.socrata.datacoordinator.truth.sql.RepBasedSqlDatasetContext
import java.util.concurrent.ExecutorService

class PerfDataSqlizer(tableName: String, datasetContext: RepBasedSqlDatasetContext[PerfType, PerfValue], executorService: ExecutorService)
  extends PostgresRepBasedDataSqlizer[PerfType, PerfValue](
    tableName,
    datasetContext,
    executorService
  )
