package com.socrata.datacoordinator
package truth.loader
package sql

import com.socrata.datacoordinator.truth.DatasetContext
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.collection.LongLikeMap

class TestDataSqlizer(tableBase: String, datasetContext: DatasetContext[TestColumnType, TestColumnValue])
  extends StandardRepBasedDataSqlizer[TestColumnType, TestColumnValue](
    tableBase,
    datasetContext,
    TestDataSqlizer.repSchemaBuilder
  )

object TestDataSqlizer {
  def repSchemaBuilder(schema: LongLikeMap[ColumnId, TestColumnType]): LongLikeMap[ColumnId, SqlColumnRep[TestColumnType, TestColumnValue]] = {
    schema.transform { (col, typ) =>
      typ match {
        case LongColumn => new LongRep(col)
        case StringColumn => new StringRep(col)
      }
    }
  }
}
