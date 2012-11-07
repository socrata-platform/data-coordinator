package com.socrata.datacoordinator.loader

import collection.mutable

object TestTypeContext extends TypeContext[TestColumnValue] {
  def makeValueFromSystemId(id: Long) = LongValue(id)
  def makeSystemIdFromValue(id: TestColumnValue) = {
    require(id.isInstanceOf[LongValue], "Not an id")
    id.asInstanceOf[LongValue].value
  }

  def isNull(v: TestColumnValue) = v == NullValue

  def makeIdSet() = new RowIdSet[TestColumnValue] {
    val s = new mutable.HashSet[TestColumnValue]

    def add(x: TestColumnValue) { s += x }

    def apply(x: TestColumnValue) = s(x)

    def clear() { s.clear() }

    def iterator = s.iterator
  }

  def makeIdMap[V]() = new RowIdMap[TestColumnValue, V] {
    val m = new mutable.HashMap[TestColumnValue, V]
    def put(x: TestColumnValue, v: V) { m += x -> v }
    def apply(x: TestColumnValue) = m(x)
    def contains(x: TestColumnValue) = m.contains(x)
  }
}
