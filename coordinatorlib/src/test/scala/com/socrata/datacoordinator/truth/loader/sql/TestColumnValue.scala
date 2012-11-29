package com.socrata.datacoordinator
package truth.loader
package sql

sealed abstract class TestColumnValue {
  def sqlize: String
}
case object NullValue extends TestColumnValue {
  def sqlize = "NULL"
}
case class LongValue(value: Long) extends TestColumnValue {
  def sqlize = value.toString
}
case class StringValue(value: String) extends TestColumnValue {
  def sqlize = "'" + value.replaceAllLiterally("'", "''") + "'"
}

