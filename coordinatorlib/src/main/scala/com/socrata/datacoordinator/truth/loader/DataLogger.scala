package com.socrata.datacoordinator
package truth.loader

import java.io.Closeable

trait DataLogger[CV] extends Closeable {
  def insert(systemID: Long, row: Row[CV])
  def update(sid: Long, row: Row[CV])
  def delete(systemID: Long)
}
