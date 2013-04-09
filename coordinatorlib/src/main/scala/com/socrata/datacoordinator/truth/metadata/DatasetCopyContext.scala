package com.socrata.datacoordinator.truth.metadata

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.ColumnName

class DatasetCopyContext[CT](val copyInfo: CopyInfo, val schema: ColumnIdMap[TypedColumnInfo[CT]]) {
  require(schema.values.forall(_.copyInfo eq copyInfo))

  def datasetInfo = copyInfo.datasetInfo

  lazy val schemaByLogicalName = schema.values.foldLeft(Map.empty[ColumnName, TypedColumnInfo[CT]]) { (acc, ci) =>
    acc + (ci.logicalName -> ci)
  }
}
