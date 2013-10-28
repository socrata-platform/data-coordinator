package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.id.RowId

trait PrevettedLoader[CV] {
  def insert(rowId: RowId, row: Row[CV])
  def update(rowId: RowId, oldRow: Option[Row[CV]], newRow: Row[CV])
  def delete(rowId: RowId, oldRow: Option[Row[CV]])
  def flush()
}
