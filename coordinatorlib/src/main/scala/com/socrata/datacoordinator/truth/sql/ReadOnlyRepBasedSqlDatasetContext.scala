package com.socrata.datacoordinator.truth.sql

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.DatasetContext

trait ReadOnlyRepBasedSqlDatasetContext[CT, CV] extends DatasetContext[CT, CV] {
  def schema: ColumnIdMap[SqlColumnReadRep[CT, CV]]

  lazy val allColumnIds = schema.keySet
  lazy val userColumnIds = allColumnIds.filterNot(systemColumnIds)
}
