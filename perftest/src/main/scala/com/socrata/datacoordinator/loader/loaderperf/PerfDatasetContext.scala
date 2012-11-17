package com.socrata.datacoordinator.loader
package loaderperf

import scala.collection.JavaConverters._

class PerfDatasetContext(val baseName: String, val userSchema: Map[String, PerfType], val userPrimaryKeyColumn: Option[String]) extends DatasetContext[PerfType, PerfValue] {
  userPrimaryKeyColumn.foreach { pkCol =>
    require(userSchema.contains(pkCol), "PK col defined but does not exist in the schema")
  }

  userSchema.keys.foreach { col =>
    require(!col.startsWith(":"), "User schema column starts with :")
  }

  def hasCopy = sys.error("hasCopy called")

  def userPrimaryKey(row: Row[PerfValue]) = for {
    userPKColumn <- userPrimaryKeyColumn
    value <- row.get(userPKColumn)
  } yield value

  def systemId(row: Row[PerfValue]) =
    row.get(systemIdColumnName).map(_.asInstanceOf[PVId].value)

  def systemIdAsValue(row: Row[PerfValue]) = row.get(systemIdColumnName)

  def systemColumns(row: Row[PerfValue]) = row.keySet.filter(_.startsWith(":"))
  val systemSchema = PerfDatasetContext.systemSchema

  val fullSchema = userSchema ++ systemSchema

  def systemIdColumnName = ":id"

  def mergeRows(a: Row[PerfValue], b: Row[PerfValue]) = a ++ b

  def makeIdSet() = {
    require(hasUserPrimaryKey)
    new RowIdSet[PerfValue] {
      val s = new java.util.HashSet[PerfValue]

      def add(x: PerfValue) { s.add(x) }

      def apply(x: PerfValue) = s.contains(x)

      def clear() { s.clear() }

      def iterator = s.iterator.asScala

      def remove(x: PerfValue) { s.remove(x) }
    }
  }

  def makeIdMap[V]() = {
    require(hasUserPrimaryKey)
    new RowIdMap[PerfValue, V] {
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

object PerfDatasetContext {
  val systemSchema = Map(":id" -> PTId)
}
