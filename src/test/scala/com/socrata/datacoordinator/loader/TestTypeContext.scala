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

    def remove(x: TestColumnValue) { s -= x }
  }

  def makeIdMap[V]() = new RowIdMap[TestColumnValue, V] {
    val m = new java.util.HashMap[TestColumnValue, V]
    def put(x: TestColumnValue, v: V) { m.put(x, v) }
    def apply(x: TestColumnValue) = { val r = m.get(x); if(r == null) throw new NoSuchElementException; r }
    def contains(x: TestColumnValue) = m.containsKey(x)

    def get(x: TestColumnValue) = Option(m.get(x))

    def clear() { m.clear() }

    def isEmpty = m.isEmpty

    def size = m.size

    def foreach(f: (TestColumnValue, V) => Unit) {
      val it = m.entrySet.iterator
      while(it.hasNext) {
        val ent = it.next()
        f(ent.getKey, ent.getValue)
      }
    }
  }
}
