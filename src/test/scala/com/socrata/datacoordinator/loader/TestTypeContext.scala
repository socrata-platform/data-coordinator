package com.socrata.datacoordinator.loader

import collection.mutable

object TestTypeContext extends TypeContext[TestColumnValue] {
  def makeSystemIdValue(id: Long) = LongValue(id)

  def isNull(v: TestColumnValue) = v == NullValue

  def makeIdObserver() = new RowIdObserver[TestColumnValue] {
    val s = new mutable.HashSet[TestColumnValue]

    def observe(x: TestColumnValue) { s += x }

    def observed(x: TestColumnValue) = s(x)

    def clear() { s.clear() }
  }
}
