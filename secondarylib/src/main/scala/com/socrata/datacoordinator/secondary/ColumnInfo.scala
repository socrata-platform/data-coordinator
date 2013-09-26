package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.id.UserColumnId

case class ColumnInfo[CT](id: UserColumnId, typ: CT, isSystemPrimaryKey: Boolean, isUserPrimaryKey: Boolean, isVersion: Boolean)
