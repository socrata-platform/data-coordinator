package com.socrata.datacoordinator.truth.reader.sql

import com.socrata.datacoordinator.truth.TypeContext
import com.socrata.datacoordinator.id.RowId

object TestTypeContext extends TypeContext[TestColumnType, TestColumnValue] {
  def isNull(value: TestColumnValue) = value eq NullValue

  def makeValueFromSystemId(id: RowId) = IdValue(id)

  def makeSystemIdFromValue(id: TestColumnValue) = id.asInstanceOf[IdValue].value

  def nullValue = NullValue

  def typeFromName(name: String) = sys.error("shouldn't call this")

  def nameFromType(typ: TestColumnType) = sys.error("shouldn't call this")
}
