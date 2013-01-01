package com.socrata.datacoordinator
package truth.loader
package sql

import com.socrata.datacoordinator.truth.sql.RepBasedSqlDatasetContext

class TestDataSqlizer(tableName: String, datasetContext: RepBasedSqlDatasetContext[TestColumnType, TestColumnValue])
  extends StandardRepBasedDataSqlizer[TestColumnType, TestColumnValue](
    tableName,
    datasetContext
  )

/*
object TestDataSqlizer {
  def repSchemaBuilder(schema: ColumnIdMap[TestColumnType]): ColumnIdMap[SqlColumnRep[TestColumnType, TestColumnValue]] = {
    schema.transform { (col, typ) =>
      typ match {
        case LongColumn => new LongRep(col)
        case StringColumn => new StringRep(col)
      }
    }
  }
}
*/
