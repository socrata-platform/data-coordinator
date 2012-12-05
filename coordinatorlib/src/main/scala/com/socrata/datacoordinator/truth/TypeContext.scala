package com.socrata.datacoordinator.truth

/** Non-dataset-specific operations on column values. */
trait TypeContext[CT, CV] {
  def isNull(value: CV): Boolean
  def makeValueFromSystemId(id: Long): CV
  def makeSystemIdFromValue(id: CV): Long
  def nullValue: CV
  def typeFromName(name: String): CT
  def nameFromType(typ: CT): String
}
