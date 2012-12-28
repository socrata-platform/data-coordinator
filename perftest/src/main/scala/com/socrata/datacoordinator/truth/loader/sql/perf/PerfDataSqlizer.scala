package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import com.socrata.datacoordinator.truth.DatasetContext
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class PerfDataSqlizer(tableName: String, datasetContext: DatasetContext[PerfType, PerfValue])
  extends PostgresRepBasedDataSqlizer[PerfType, PerfValue](
    tableName,
    datasetContext,
    PerfDataSqlizer.repSchemaBuilder
  )

object PerfDataSqlizer {
  def repSchemaBuilder(schema: ColumnIdMap[PerfType]): ColumnIdMap[SqlColumnRep[PerfType, PerfValue]] =
    schema.transform { (col, typ) =>
      typ match {
        case PTId => new IdRep(col)
        case PTNumber => new NumberRep(col)
        case PTText => new TextRep(col)
      }
    }
}
