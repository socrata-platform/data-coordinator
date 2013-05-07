package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.id.{RowVersion, RowId}
import com.socrata.datacoordinator.util.collection.ColumnIdSet

trait RowPreparer[CV] {
  def prepareForInsert(row: Row[CV], sid: RowId): Either[RowPreparerDeclinedUpsert[CV], Row[CV]]
  def prepareForUpdate(row: Row[CV], oldRow: Row[CV]): Either[RowPreparerDeclinedUpsert[CV], Row[CV]]
  def prepareForDelete(id: CV, requestedVersion: Option[RowVersion], existingVersion: RowVersion): Option[RowPreparerDeclinedUpsert[CV]]
}
