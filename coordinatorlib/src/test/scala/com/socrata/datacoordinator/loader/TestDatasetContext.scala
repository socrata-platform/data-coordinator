package com.socrata.datacoordinator.loader

import scala.collection.JavaConverters._

class TestDatasetContext(val baseName: String, val userSchema: Map[String, TestColumnType], val userPrimaryKeyColumn: Option[String]) extends DatasetContext[TestColumnType, TestColumnValue] {
  userPrimaryKeyColumn.foreach { pkCol =>
    require(userSchema.contains(pkCol), "PK col defined but does not exist in the schema")
  }

  userSchema.keys.foreach { col =>
    require(!col.startsWith(":"), "User schema column starts with :")
  }

  def hasCopy = sys.error("hasCopy called")

  def userPrimaryKey(row: Row[TestColumnValue]) = for {
    userPKColumn <- userPrimaryKeyColumn
    value <- row.get(userPKColumn)
  } yield value

  def systemId(row: Row[TestColumnValue]) =
    row.get(systemIdColumnName).map(_.asInstanceOf[LongValue].value)

  def systemIdAsValue(row: Row[TestColumnValue]) = row.get(systemIdColumnName)

  def systemColumns(row: Row[TestColumnValue]) = row.keySet.filter(_.startsWith(":"))
  val systemSchema = TestDatasetContext.systemSchema

  val fullSchema = userSchema ++ systemSchema

  def systemIdColumnName = ":id"

  def mergeRows(a: Row[TestColumnValue], b: Row[TestColumnValue]) = a ++ b


  def makeIdSet() = {
    require(hasUserPrimaryKey)
    new RowIdSet[TestColumnValue] {
      val s = new java.util.HashSet[TestColumnValue]

      def add(x: TestColumnValue) { s.add(x) }

      def apply(x: TestColumnValue) = s.contains(x)

      def clear() { s.clear() }

      def iterator = s.iterator.asScala

      def remove(x: TestColumnValue) { s.remove(x) }
    }
  }

  def makeIdMap[V]() = {
    require(hasUserPrimaryKey)
    new RowIdMap[TestColumnValue, V] {
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
}

object TestDatasetContext {
  val systemSchema = Map(":id" -> LongColumn)
}
