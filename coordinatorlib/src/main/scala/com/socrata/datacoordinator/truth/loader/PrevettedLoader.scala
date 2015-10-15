package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.id.RowId

trait PrevettedLoader[CV] {
  def insert(rowId: RowId, row: Row[CV]): Unit
  def update(rowId: RowId, oldRow: Option[Row[CV]], newRow: Row[CV]): Unit
  def delete(rowId: RowId, oldRow: Option[Row[CV]]): Unit
  def flush(): Unit
}
