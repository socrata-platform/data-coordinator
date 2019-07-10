package com.socrata.datacoordinator
package truth.loader
package sql

import scala.collection.mutable

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.id.RowId

class SqlPrevettedLoader[CT, CV](val conn: Connection, sqlizer: DataSqlizer[CT, CV], logger: DataLogger[CV]) extends PrevettedLoader[CV] {
  val typeContext = sqlizer.datasetContext.typeContext

  val insertBatch = new mutable.ArrayBuffer[Insert[CV]]
  val updateBatch = new mutable.ArrayBuffer[Update[CV]]
  val deleteBatch = new mutable.ArrayBuffer[Delete[CV]]

  def insert(rowId: RowId, row: Row[CV]) {
    flushUpdates()
    flushDeletes()
    insertBatch += Insert(rowId, row)
    logger.insert(rowId, row)
  }

  def update(rowId: RowId, oldRow: Option[Row[CV]], newRow: Row[CV]) {
    flushInserts()
    flushDeletes()
    updateBatch += Update(rowId, oldRow, newRow)
    logger.update(rowId, oldRow, newRow)
  }

  def delete(rowId: RowId, oldRow: Option[Row[CV]]) {
    flushInserts()
    flushUpdates()
    deleteBatch += Delete(rowId, oldRow)
    logger.delete(rowId, oldRow)
  }

  def flushUpdates() {
    if(updateBatch.nonEmpty) {
      try {
        val (updated, ()) = sqlizer.updateBatch(conn) { updater =>
          for(update <- updateBatch) {
            updater.update(update.systemId, update.data)
          }
        }
        assert(updated == updateBatch.size, "Pre-vetted update didn't affect exactly " + updateBatch.size + " row(s)?")
      } finally {
        updateBatch.clear()
      }
    }
  }

  def flushDeletes() {
    if(deleteBatch.nonEmpty) {
      try {
        val (deleted, ()) = sqlizer.deleteBatch(conn) { deleter =>
          for(delete <- deleteBatch) { deleter.delete(delete.systemId) }
        }
        assert(deleted == deleteBatch.size, "Expected " + deleteBatch.size + " rows to be deleted, but only found " + deleted)
      } finally {
        deleteBatch.clear()
      }
    }
  }

  def flushInserts() {
    if(insertBatch.nonEmpty) {
      try {
        sqlizer.insertBatch(conn) { inserter =>
          for(insert <- insertBatch) {
            inserter.insert(insert.data)
          }
        }
      } finally {
        insertBatch.clear()
      }
    }
  }

  def flush() {
    flushInserts()
    flushUpdates()
    flushDeletes()
  }
}
