package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}

case class ColumnInfo[CT](systemId: ColumnId, id: UserColumnId, typ: CT, isSystemPrimaryKey: Boolean, isUserPrimaryKey: Boolean, isVersion: Boolean)
