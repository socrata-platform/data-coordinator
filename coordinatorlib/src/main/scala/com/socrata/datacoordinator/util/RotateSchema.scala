package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.AbstractColumnInfoLike
import com.socrata.soql.environment.ColumnName

object RotateSchema {
  def apply[T <: AbstractColumnInfoLike](schema: ColumnIdMap[T]): Map[ColumnName, T] = {
    schema.values.foldLeft(Map.empty[ColumnName, T]) { (acc, colInfo) =>
      acc + (colInfo.logicalName -> colInfo)
    }
  }
}
