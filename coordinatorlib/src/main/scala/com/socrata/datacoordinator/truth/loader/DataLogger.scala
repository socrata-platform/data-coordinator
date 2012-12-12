package com.socrata.datacoordinator
package truth.loader

import java.io.Closeable

trait DataLogger[CV] extends Closeable {
  def insert(systemID: RowId, row: Row[CV])
  def update(systemID: RowId, row: Row[CV])
  def delete(systemID: RowId)
}
