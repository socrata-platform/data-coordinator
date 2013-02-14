package com.socrata.datacoordinator
package secondary

import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.DatasetId

trait Secondary[CV] {
  def wantsSnapshots: Boolean

  def currentVersion(datasetInfo: DatasetId): Long
  def version(copyInfo: CopyInfo, events: Iterator[Delogger.LogEvent[CV]])
  def resync(copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo], rows: Iterator[Row[CV]])
}
