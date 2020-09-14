package com.socrata.datacoordinator.truth.metadata

import com.rojoma.json.v3.ast.JObject
import com.socrata.soql.environment.ColumnName

case class IndexDirectives(datasetInfo: DatasetInfo, fieldName: ColumnName, directives: JObject)
