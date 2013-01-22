package com.socrata.datacoordinator
package truth.loader

import java.io.Closeable
import com.socrata.datacoordinator.id.RowId

trait PrevettedLoader[CV] extends Closeable {
  def insert(rowId: RowId, row: Row[CV])
  def update(rowId: RowId, row: Row[CV])
  def delete(rowId: RowId)
}
