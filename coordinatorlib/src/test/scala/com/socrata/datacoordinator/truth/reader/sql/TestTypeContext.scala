package com.socrata.datacoordinator.truth.reader.sql

import com.socrata.datacoordinator.truth.{RowUserIdMap, TypeContext}
import com.socrata.datacoordinator.id.{RowVersion, RowId}
import com.socrata.soql.environment.TypeName

object TestTypeContext extends TypeContext[TestColumnType, TestColumnValue] {
  def isNull(value: TestColumnValue) = value eq NullValue

  def makeValueFromSystemId(id: RowId) = IdValue(id)

  def makeSystemIdFromValue(id: TestColumnValue) = id.asInstanceOf[IdValue].value

  def makeValueFromRowVersion(v: RowVersion) = VersionValue(v)

  def makeRowVersionFromValue(v: TestColumnValue) = v.asInstanceOf[VersionValue].value

  def nullValue = NullValue

  lazy val typeNamespace = sys.error("shouldn't call this")

  def makeIdMap[T](ignored: TestColumnType) = new RowUserIdMap[TestColumnValue, T] {
    val underlying = new scala.collection.mutable.HashMap[String, T]

    def s(x: TestColumnValue) = x.asInstanceOf[StringValue].value

    def put(x: TestColumnValue, v: T) {
      underlying += s(x) -> v
    }

    def apply(x: TestColumnValue) = underlying(s(x))

    def get(x: TestColumnValue) = underlying.get(s(x))

    def clear() { underlying.clear() }

    def contains(x: TestColumnValue) = underlying.contains(s(x))

    def isEmpty = underlying.isEmpty

    def size = underlying.size

    def foreach(f: (TestColumnValue, T) => Unit) {
      underlying.foreach { case (k,v) =>
        f(StringValue(k), v)
      }
    }

    def valuesIterator = underlying.valuesIterator

    def remove(x: TestColumnValue) {
      underlying.remove(s(x))
    }
  }
}
