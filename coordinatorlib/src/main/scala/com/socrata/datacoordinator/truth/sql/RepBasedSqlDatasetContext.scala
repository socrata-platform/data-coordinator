package com.socrata.datacoordinator.truth
package sql

import com.socrata.datacoordinator.util.collection.ColumnIdMap

trait RepBasedSqlDatasetContext[CT, CV] extends DatasetContext[CT, CV] {
  val schema: ColumnIdMap[SqlColumnRep[CT, CV]]
  lazy val allColumnIds = schema.keySet
  lazy val userColumnIds = allColumnIds.filterNot(systemColumnIds)

  lazy val canonicallyOrderedColumnIds = allColumnIds.toSet.toSeq.sorted
  lazy val canonicallyOrderedUserColumnIds = userColumnIds.toSet.toSeq.sorted
}
