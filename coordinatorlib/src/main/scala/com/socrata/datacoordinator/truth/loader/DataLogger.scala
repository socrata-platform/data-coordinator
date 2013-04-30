package com.socrata.datacoordinator
package truth.loader

import java.io.Closeable
import com.socrata.datacoordinator.id.RowId

trait DataLogger[CV] extends Closeable {
  def insert(systemID: RowId, row: Row[CV])
  def update(systemID: RowId, row: Row[CV])
  def delete(systemID: RowId)
  def counterUpdated(nextCounter: Long)
}
