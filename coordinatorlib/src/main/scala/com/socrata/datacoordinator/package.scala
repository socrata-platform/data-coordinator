package com.socrata

import com.socrata.datacoordinator.util.collection.{MutableColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.id.ColumnId

package object datacoordinator {
  type Row[ColumnValue] = ColumnIdMap[ColumnValue]
  type MutableRow[ColumnValue] = MutableColumnIdMap[ColumnValue]
  def Row[ColumnValue](xs: (ColumnId, ColumnValue)*) = ColumnIdMap[ColumnValue](xs: _*)
}
