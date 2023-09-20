package com.socrata.datacoordinator.truth

import com.socrata.soql.analyzer2.MetaTypes

import com.socrata.datacoordinator.id.DatasetInternalName

final abstract class UnstagedDataCoordinatorMetaTypes extends MetaTypes {
  type DatabaseTableNameImpl = DatasetInternalName

  type ResourceNameScope = DataCoordinatorMetaTypes#ResourceNameScope
  type ColumnType = DataCoordinatorMetaTypes#ColumnType
  type ColumnValue = DataCoordinatorMetaTypes#ColumnValue
  type DatabaseColumnNameImpl = DataCoordinatorMetaTypes#DatabaseColumnNameImpl
}
