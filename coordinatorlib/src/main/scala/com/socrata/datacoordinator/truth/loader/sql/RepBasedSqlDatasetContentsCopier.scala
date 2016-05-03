package com.socrata.datacoordinator.truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.truth.sql.SqlColumnWriteRep
import com.socrata.datacoordinator.util.TimingReport

class RepBasedSqlDatasetContentsCopier[CT, CV](conn: Connection, logger: Logger[CT, CV], repFor: ColumnInfo[CT] => SqlColumnWriteRep[CT, CV], timingReport: TimingReport) extends DatasetContentsCopier[CT] {
  def copy(from: CopyInfo, to: DatasetCopyContext[CT]) {
    require(from.datasetInfo == to.datasetInfo, "Cannot copy across datasets")
    if(to.schema.nonEmpty) {
      val physCols = to.schema.values.flatMap(repFor(_).physColumns).mkString(",")
      using(conn.createStatement()) { stmt =>
        timingReport("copy-dataset-contents", "from-copy" -> from.systemId, "to-copy" -> to.copyInfo.systemId) {
          stmt.execute(s"INSERT INTO ${to.copyInfo.dataTableName} ($physCols) SELECT $physCols FROM ${from.dataTableName}")
        }
        timingReport("analyze-post-copy", "to-copy" -> to.copyInfo.systemId)
        stmt.execute(s"ANALYZE ${to.copyInfo.dataTableName}")
      }
      logger.dataCopied()
    }
  }
}
