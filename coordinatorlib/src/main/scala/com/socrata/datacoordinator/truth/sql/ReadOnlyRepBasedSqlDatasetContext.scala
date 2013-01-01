package com.socrata.datacoordinator.truth.sql

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.DatasetContext

trait ReadOnlyRepBasedSqlDatasetContext[CT, CV] extends DatasetContext[CT, CV] {
  def schema: ColumnIdMap[SqlColumnReadRep[CT, CV]]

  lazy val fullSchema = schema.mapValuesStrict(_.representedType)

  lazy val userSchema = fullSchema.filterNot { (cid, _) => systemColumnSet(cid) }
  lazy val systemSchema = fullSchema.filter { (cid, _) => systemColumnSet(cid) }
}
