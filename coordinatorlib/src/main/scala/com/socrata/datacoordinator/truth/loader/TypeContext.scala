package com.socrata.datacoordinator
package truth.loader

/** Non-dataset-specific operations on column values. */
trait TypeContext[CV] {
  def isNull(value: CV): Boolean
  def makeValueFromSystemId(id: Long): CV
  def makeSystemIdFromValue(id: CV): Long
  def nullValue: CV
}
