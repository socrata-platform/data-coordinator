package com.socrata.datacoordinator.truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.sql.SqlColumnWriteRep
import com.socrata.datacoordinator.util.TimingReport

class RepBasedSqlDatasetContentsCopier[CT, CV](conn: Connection, logger: Logger[CV], repFor: ColumnInfo => SqlColumnWriteRep[CT, CV], timingReport: TimingReport) extends DatasetContentsCopier {
  def copy(from: CopyInfo, to: CopyInfo, schema: ColumnIdMap[ColumnInfo]) {
    if(schema.nonEmpty) {
    val physCols = schema.values.flatMap(repFor(_).physColumns).mkString(",")
      using(conn.createStatement()) { stmt =>
        timingReport("copy-dataset-contents", "from-copy" -> from.systemId, "to-copy" -> to.systemId) {
          stmt.execute(s"INSERT INTO ${to.dataTableName} ($physCols) SELECT $physCols FROM ${from.dataTableName}")
        }
        // TODO: schedule target table for a VACUUM ANALYZE (since it can't happen in a transaction)
      }
      logger.dataCopied()
    }
  }
}
