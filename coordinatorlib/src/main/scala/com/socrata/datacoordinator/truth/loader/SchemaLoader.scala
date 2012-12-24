package com.socrata.datacoordinator
package truth.loader

trait SchemaLoader[CT, CV] {
  def create()

  def addColumn(baseName: String, typ: CT)
  def dropColumn(baseName: String, typ: CT)

  def makePrimaryKey(baseName: String, typ: CT): Boolean // false if this type cannot be used as a PK by the database
  def dropPrimaryKey(baseName: String, typ: CT): Boolean
}
