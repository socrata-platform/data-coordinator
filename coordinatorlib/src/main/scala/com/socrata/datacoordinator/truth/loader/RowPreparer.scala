package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.id.{RowVersion, RowId}
import com.socrata.datacoordinator.util.collection.ColumnIdSet

trait RowPreparer[CV] {
  def prepareForInsert(row: Row[CV], sid: RowId, version: RowVersion): Row[CV]
  def prepareForUpdate(row: Row[CV], oldRow: Row[CV], newVersion: RowVersion): Row[CV]
}
