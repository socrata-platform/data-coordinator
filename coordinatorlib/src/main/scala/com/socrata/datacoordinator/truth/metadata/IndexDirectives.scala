package com.socrata.datacoordinator.truth.metadata

import com.rojoma.json.v3.ast.JObject

case class IndexDirective[CT](copyInfo: CopyInfo, columnInfo: ColumnInfo[CT], directive: JObject)
