package com.socrata.datacoordinator
package truth

import com.socrata.datacoordinator.id.RowId

/** Non-dataset-specific operations on column values. */
trait TypeContext[CT, CV] {
  def isNull(value: CV): Boolean
  def makeValueFromSystemId(id: RowId): CV
  def makeSystemIdFromValue(id: CV): RowId
  def nullValue: CV
  def typeFromNameOpt(name: String): Option[CT]
  def nameFromType(typ: CT): String
  def makeIdMap[T](idColumnType: CT): RowUserIdMap[CV, T]

  def typeFromName(name: String): CT = typeFromNameOpt(name) match {
    case Some(t) => t
    case None => throw new IllegalArgumentException("Unknown type " + name)
  }
}
