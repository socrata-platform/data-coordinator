package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import scala.collection.JavaConverters._

import com.socrata.datacoordinator.truth.{RowUserIdMap, TypeContext}
import com.socrata.datacoordinator.id.RowId

object PerfTypeContext extends TypeContext[PerfType, PerfValue] {
  def isNull(value: PerfValue) = value eq PVNull

  def makeValueFromSystemId(id: RowId) = PVId(id)

  def makeSystemIdFromValue(id: PerfValue) = id match {
    case PVId(x) => x
    case _ => sys.error("Not an id: " + id)
  }

  val nullValue = PVNull

  def typeFromNameOpt(name: String) = name match {
    case "id" => Some(PTId)
    case "number" => Some(PTNumber)
    case "text" => Some(PTText)
    case _ => None
  }

  def nameFromType(typ: PerfType) = typ match {
    case PTId => "id"
    case PTNumber => "number"
    case PTText => "text"
  }

  def makeIdMap[V](ignored: PerfType) = {
    new RowUserIdMap[PerfValue, V] {
      val m = new java.util.HashMap[PerfValue, V]
      def put(x: PerfValue, v: V) { m.put(x, v) }
      def apply(x: PerfValue) = { val r = m.get(x); if(r == null) throw new NoSuchElementException; r }
      def contains(x: PerfValue) = m.containsKey(x)

      def get(x: PerfValue) = Option(m.get(x))

      def clear() { m.clear() }

      def isEmpty = m.isEmpty

      def size = m.size

      def foreach(f: (PerfValue, V) => Unit) {
        val it = m.entrySet.iterator
        while(it.hasNext) {
          val ent = it.next()
          f(ent.getKey, ent.getValue)
        }
      }

      def valuesIterator = m.values.iterator.asScala
    }
  }
}
