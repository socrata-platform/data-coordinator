package com.socrata.datacoordinator
package truth.loader
package sql

import scala.collection.mutable

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.id.RowId

class SqlPrevettedLoader[CT, CV](val conn: Connection, sqlizer: DataSqlizer[CT, CV], logger: DataLogger[CV]) extends PrevettedLoader[CV] {

  import SqlPrevettedLoader._

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
        insertBatch.grouped(BatchSize).foreach { subBatch =>
          sqlizer.insertBatch(conn) { inserter =>
            for(insert <- subBatch) {
              inserter.insert(insert.data)
            }
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

object SqlPrevettedLoader {

  private val BatchSize = 1000

}
