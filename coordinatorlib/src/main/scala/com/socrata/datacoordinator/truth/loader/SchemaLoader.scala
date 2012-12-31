package com.socrata.datacoordinator
package truth.loader

import com.socrata.datacoordinator.truth.metadata.DatasetMapWriter

trait SchemaLoader {
  def create()

  def addColumn(colInfo: DatasetMapWriter#ColumnInfo)
  def dropColumn(colInfo: DatasetMapWriter#ColumnInfo)

  def makePrimaryKey(colInfo: DatasetMapWriter#ColumnInfo): Boolean // false if this type cannot be used as a PK by the database
  def dropPrimaryKey(colInfo: DatasetMapWriter#ColumnInfo): Boolean
}
