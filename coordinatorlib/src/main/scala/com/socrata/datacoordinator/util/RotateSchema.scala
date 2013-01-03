package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo

object RotateSchema {
  def apply[T <: ColumnInfo](schema: ColumnIdMap[T]): Map[String, T] = {
    schema.values.foldLeft(Map.empty[String, T]) { (acc, colInfo) =>
      acc + (colInfo.logicalName -> colInfo)
    }
  }
}
