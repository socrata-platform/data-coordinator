package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.util.collection.{MutableUserColumnIdMap, UserColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.truth.metadata.AbstractColumnInfoLike

object RotateSchema {
  def apply[T <: AbstractColumnInfoLike](schema: ColumnIdMap[T]): UserColumnIdMap[T] = {
    val res = new MutableUserColumnIdMap[T]()
    schema.foreach { (_, colInfo) =>
      res += (colInfo.userColumnId -> colInfo)
    }
    res.freeze()
  }
}
