package com.socrata.datacoordinator.secondary

import com.rojoma.json.v3.ast.JObject
import com.socrata.datacoordinator.id.{StrategyType, ColumnId, UserColumnId}
import com.socrata.soql.environment.ColumnName

case class ColumnInfo[CT](systemId: ColumnId, id: UserColumnId, fieldName: Option[ColumnName], typ: CT, isSystemPrimaryKey: Boolean,
                          isUserPrimaryKey: Boolean, isVersion: Boolean, computationStrategyInfo: Option[ComputationStrategyInfo])

case class ComputationStrategyInfo(strategyType: StrategyType, sourceColumnIds: Seq[UserColumnId], parameters: JObject)
