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
  val deleteBatch = new mutable.ArrayBuffer[Delete]

  def insert(rowId: RowId, row: Row[CV]) {
    flushUpdates()
    flushDeletes()
    insertBatch += Insert(rowId, row)
    logger.insert(rowId, row)
  }

  def update(rowId: RowId, row: Row[CV]) {
    flushInserts()
    flushDeletes()
    updateBatch += Update(rowId, row)
    logger.update(rowId, row)
  }

  def delete(rowId: RowId) {
    flushInserts()
    flushUpdates()
    deleteBatch += Delete(rowId)
    logger.delete(rowId)
  }

  def flushUpdates() {
    if(updateBatch.nonEmpty) {
      try {
        for(stmt <- managed(conn.prepareStatement(sqlizer.prepareSystemIdUpdateStatement))) {
          for(update <- updateBatch) {
            sqlizer.prepareSystemIdUpdate(stmt, update.systemId, update.data)
            stmt.addBatch()
          }
          checkResults(stmt.executeBatch(), 1)
        }
      } finally {
        updateBatch.clear()
      }
    }
  }

  def checkResults(updated: Array[Int], expected: Int) {
    var i = 0
    while(i < updated.length) {
      assert(updated(i) == expected, "Pre-vetted update didn't affect exactly " + expected + " row(s)?")
      i += 1
    }
  }

  def flushDeletes() {
    if(deleteBatch.nonEmpty) {
      try {
        val deleted = sqlizer.deleteBatch(conn) { deleter =>
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
