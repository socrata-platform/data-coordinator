package com.socrata.datacoordinator.truth.loader

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, VersionInfo}
import com.socrata.datacoordinator.util.collection.ColumnIdMap

trait DatasetContentsCopier {
  def copy(from: VersionInfo, to: VersionInfo, schema: ColumnIdMap[ColumnInfo])
}
