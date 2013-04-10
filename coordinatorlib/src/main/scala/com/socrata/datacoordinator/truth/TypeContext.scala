package com.socrata.datacoordinator
package truth

import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.metadata.TypeNamespace

/** Non-dataset-specific operations on column values. */
trait TypeContext[CT, CV] {
  def isNull(value: CV): Boolean
  def makeValueFromSystemId(id: RowId): CV
  def makeSystemIdFromValue(id: CV): RowId
  def nullValue: CV
  def makeIdMap[T](idColumnType: CT): RowUserIdMap[CV, T]
  val typeNamespace: TypeNamespace[CT]
}
