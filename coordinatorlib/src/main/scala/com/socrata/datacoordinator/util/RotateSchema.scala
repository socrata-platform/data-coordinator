package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.soql.environment.ColumnName

object RotateSchema {
  def apply[T <: ColumnInfo](schema: ColumnIdMap[T]): Map[ColumnName, T] = {
    schema.values.foldLeft(Map.empty[ColumnName, T]) { (acc, colInfo) =>
      acc + (colInfo.logicalName -> colInfo)
    }
  }
}
