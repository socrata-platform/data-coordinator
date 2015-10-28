package com.socrata.datacoordinator
package truth.loader

import java.io.Closeable
import com.socrata.datacoordinator.id.RowId

trait DataLogger[CV] extends Closeable {
  def insert(systemID: RowId, row: Row[CV]): Unit
  def update(systemID: RowId, oldRow: Option[Row[CV]], newRow: Row[CV]): Unit
  def delete(systemID: RowId, oldRow: Option[Row[CV]]): Unit
  def counterUpdated(nextCounter: Long): Unit
}
