package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}

trait SchemaLoader {
  def create(copyInfo: CopyInfo)

  def addColumn(colInfo: ColumnInfo)
  def dropColumn(colInfo: ColumnInfo)

  def makePrimaryKey(colInfo: ColumnInfo): Boolean // false if this type cannot be used as a PK by the database
  def makeSystemPrimaryKey(colInfo: ColumnInfo): Boolean // false if this type cannot be used as a PK by the database
  def dropPrimaryKey(colInfo: ColumnInfo): Boolean
}
