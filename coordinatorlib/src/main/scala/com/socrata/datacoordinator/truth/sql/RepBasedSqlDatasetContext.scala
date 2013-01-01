package com.socrata.datacoordinator.truth
package sql

import com.socrata.datacoordinator.util.collection.ColumnIdMap

trait RepBasedSqlDatasetContext[CT, CV] extends DatasetContext[CT, CV] {
  def schema: ColumnIdMap[SqlColumnRep[CT, CV]]

  lazy val fullSchema = schema.mapValuesStrict(_.representedType)

  lazy val userSchema = fullSchema.filterNot { (cid, _) => systemColumnSet(cid) }
  lazy val systemSchema = fullSchema.filter { (cid, _) => systemColumnSet(cid) }
}
