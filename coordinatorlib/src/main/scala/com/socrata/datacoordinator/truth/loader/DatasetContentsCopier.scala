package com.socrata.datacoordinator.truth.loader

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.util.collection.ColumnIdMap

trait DatasetContentsCopier {
  def copy(from: CopyInfo, to: CopyInfo, schema: ColumnIdMap[ColumnInfo])
}
