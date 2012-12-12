package com.socrata.datacoordinator
package truth

/** Non-dataset-specific operations on column values. */
trait TypeContext[CT, CV] {
  def isNull(value: CV): Boolean
  def makeValueFromSystemId(id: RowId): CV
  def makeSystemIdFromValue(id: CV): RowId
  def nullValue: CV
  def typeFromName(name: String): CT
  def nameFromType(typ: CT): String
}
