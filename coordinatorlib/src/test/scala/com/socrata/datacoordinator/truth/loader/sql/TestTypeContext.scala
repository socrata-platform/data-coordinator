package com.socrata.datacoordinator
package truth.loader
package sql

import com.socrata.datacoordinator.truth.TypeContext
import com.socrata.datacoordinator.id.RowId

object TestTypeContext extends TypeContext[TestColumnType, TestColumnValue] {
  def makeValueFromSystemId(id: RowId) = LongValue(id.underlying)
  def makeSystemIdFromValue(id: TestColumnValue) = {
    require(id.isInstanceOf[LongValue], "Not an id")
    new RowId(id.asInstanceOf[LongValue].value)
  }

  def isNull(v: TestColumnValue) = v == NullValue
  def nullValue = NullValue

  val types = Map(
    "long" -> LongColumn,
    "string" -> StringColumn
  )

  def typeFromName(name: String) = types(name)
  def nameFromType(typ: TestColumnType) = typ match {
    case LongColumn => "long"
    case StringColumn => "string"
  }
}
