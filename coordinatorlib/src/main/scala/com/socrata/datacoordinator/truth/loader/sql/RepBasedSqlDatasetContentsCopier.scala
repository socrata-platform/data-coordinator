package com.socrata.datacoordinator.truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.v2._

import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.truth.sql.SqlColumnWriteRep
import com.socrata.datacoordinator.util.TimingReport

class RepBasedSqlDatasetContentsCopier[CT, CV](conn: Connection, logger: Logger[CT, CV], repFor: ColumnInfo[CT] => SqlColumnWriteRep[CT, CV], timingReport: TimingReport) extends DatasetContentsCopier[CT] {
  def copy(from: DatasetCopyContext[CT], to: DatasetCopyContext[CT]) {
    require(from.datasetInfo == to.datasetInfo, "Cannot copy across datasets")
    if(to.schema.nonEmpty) {
      val toPhysCols = to.schema.values.flatMap(repFor(_).physColumns).mkString(",")
      // Same columns in the same order...
      val fromPhysCols = to.schema.keys.flatMap { cid =>
        repFor(from.schema(cid)).physColumns
      }.mkString(",")

      using(conn.createStatement()) { stmt =>
        timingReport("copy-dataset-contents", "from-copy" -> from.copyInfo.systemId, "to-copy" -> to.copyInfo.systemId) {
          stmt.execute(s"INSERT INTO ${to.copyInfo.dataTableName} ($toPhysCols) SELECT $fromPhysCols FROM ${from.copyInfo.dataTableName}")
        }
        timingReport("analyze-post-copy", "to-copy" -> to.copyInfo.systemId) {
          stmt.execute(s"ANALYZE ${to.copyInfo.dataTableName}")
        }
      }
      logger.dataCopied()
    }
  }
}
