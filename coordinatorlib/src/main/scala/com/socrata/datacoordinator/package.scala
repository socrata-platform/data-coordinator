package com.socrata

import com.socrata.datacoordinator.util.collection.{LongLikeMap, MutableLongLikeMap}

package object datacoordinator {
  // TODO: Make all of these value classes!
  type DatasetId = Long
  def DatasetId(x: Long): DatasetId = x

  type VersionId = Long
  def VersionId(x: Long): VersionId = x

  type ColumnId = Long
  def ColumnId(x: Long): ColumnId = x

  type RowId = Long
  def RowId(x: Long): RowId = x

  type Row[ColumnValue] = LongLikeMap[ColumnId, ColumnValue]
  type MutableRow[ColumnValue] = MutableLongLikeMap[ColumnId, ColumnValue]
  def Row[ColumnValue](xs: (ColumnId, ColumnValue)*) = LongLikeMap[ColumnId, ColumnValue](xs: _*)
}
