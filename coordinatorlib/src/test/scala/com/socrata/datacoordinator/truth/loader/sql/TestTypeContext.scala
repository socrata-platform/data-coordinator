package com.socrata.datacoordinator
package truth.loader
package sql

import com.socrata.datacoordinator.truth.TypeContext

object TestTypeContext extends TypeContext[TestColumnValue] {
  def makeValueFromSystemId(id: Long) = LongValue(id)
  def makeSystemIdFromValue(id: TestColumnValue) = {
    require(id.isInstanceOf[LongValue], "Not an id")
    id.asInstanceOf[LongValue].value
  }

  def isNull(v: TestColumnValue) = v == NullValue
  def nullValue = NullValue
}
