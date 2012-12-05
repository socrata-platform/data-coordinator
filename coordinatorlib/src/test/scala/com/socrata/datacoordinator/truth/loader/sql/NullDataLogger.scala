package com.socrata.datacoordinator
package truth.loader.sql

import com.socrata.datacoordinator.truth.loader.DataLogger

object NullDataLogger extends DataLogger[TestColumnValue] {
  def insert(systemID: Long, row: Row[TestColumnValue]) {}

  def update(sid: Long, row: Row[TestColumnValue]) {}

  def delete(systemID: Long) {}

  def finish() = 0L

  def close() {}
}
