package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import java.sql.{Connection, PreparedStatement}

import org.postgresql.core.BaseConnection
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{LeakDetect, FastGroupedIterator, CloseableIterator, StringBuilderReader}
import com.socrata.datacoordinator.truth.DatasetContext
import util.collection.LongLikeMap
import truth.sql.SqlColumnRep

class PerfDataSqlizer(tableName: String, datasetContext: DatasetContext[PerfType, PerfValue])
  extends PostgresRepBasedDataSqlizer[PerfType, PerfValue](
    tableName,
    datasetContext,
    PerfDataSqlizer.repSchemaBuilder
  )

object PerfDataSqlizer {
  def repSchemaBuilder(schema: LongLikeMap[ColumnId, PerfType]): LongLikeMap[ColumnId, SqlColumnRep[PerfType, PerfValue]] =
    schema.transform { (col, typ) =>
      typ match {
        case PTId => new IdRep(col)
        case PTNumber => new NumberRep(col)
        case PTText => new TextRep(col)
      }
    }
}
