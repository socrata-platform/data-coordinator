package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import scala.collection.JavaConverters._

import com.socrata.datacoordinator.truth.{RowIdMap, DatasetContext}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.truth.sql.{RepBasedSqlDatasetContext, SqlColumnRep}

class PerfDatasetContext(val schema: ColumnIdMap[SqlColumnRep[PerfType, PerfValue]], val systemIdColumn: ColumnId, val userPrimaryKeyColumn: Option[ColumnId]) extends RepBasedSqlDatasetContext[PerfType, PerfValue] {
  val typeContext = PerfTypeContext

  userPrimaryKeyColumn.foreach { pkCol =>
    require(schema.contains(pkCol), "PK col defined but does not exist in the schema")
  }

  def userPrimaryKey(row: Row[PerfValue]) = for {
    userPKColumn <- userPrimaryKeyColumn
    value <- row.get(userPKColumn)
  } yield value

  def systemId(row: Row[PerfValue]) =
    row.get(systemIdColumn).map(_.asInstanceOf[PVId].value)

  def systemIdAsValue(row: Row[PerfValue]) = row.get(systemIdColumn)

  def systemColumns(row: Row[PerfValue]) = if(row.contains(systemIdColumn)) Set(systemIdColumn) else Set.empty

  val systemColumnSet = Set(systemIdColumn)

  def mergeRows(a: Row[PerfValue], b: Row[PerfValue]) = a ++ b

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
