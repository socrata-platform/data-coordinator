package com.socrata.datacoordinator.truth

import com.socrata.soql.analyzer2.MetaTypes
import com.socrata.soql.types.{SoQLType, SoQLValue}

import com.socrata.datacoordinator.id.{DatasetInternalName, UserColumnId}
import com.socrata.datacoordinator.truth.metadata.LifecycleStage

final abstract class DataCoordinatorMetaTypes extends MetaTypes {
  type ResourceNameScope = Int
  type ColumnType = SoQLType
  type ColumnValue = SoQLValue
  type DatabaseTableNameImpl = (DatasetInternalName, LifecycleStage)
  type DatabaseColumnNameImpl = UserColumnId
}
