package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.util.collection.ColumnIdSet

trait RowPreparer[CV] {
  def prepareForInsert(row: Row[CV], sid: RowId): Either[RowPreparerDeclinedUpsert[CV], Row[CV]]
  def prepareForUpdate(row: Row[CV], oldRow: Row[CV]): Either[RowPreparerDeclinedUpsert[CV], Row[CV]]
  def columnsRequiredForDelete: ColumnIdSet
  def prepareForDelete(row: Row[CV], sid: RowId, version: Option[CV]): Option[RowPreparerDeclinedUpsert[CV]]
}
