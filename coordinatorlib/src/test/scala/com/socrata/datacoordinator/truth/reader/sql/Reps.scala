package com.socrata.datacoordinator
package truth.reader.sql

import com.socrata.datacoordinator.truth.sql.{SqlColumnReadRep, SqlPKableColumnReadRep}
import java.sql.{PreparedStatement, ResultSet}
import java.lang.StringBuilder
import com.socrata.datacoordinator.id.{RowId, ColumnId}

class IdRep(cid: ColumnId) extends SqlPKableColumnReadRep[TestColumnType, TestColumnValue] {
  def representedType = IdType

  val base = "c_" + cid.underlying

  def physColumns = Array(base)

  def sqlTypes = Array("BIGINT")

  def fromResultSet(rs: ResultSet, start: Int) =
    IdValue(new RowId(rs.getLong(start)))

  def templateForMultiLookup(n: Int) = (1 to n).map(_ => "?").mkString("(" + base + " in (", ",", "))")

  def prepareMultiLookup(stmt: PreparedStatement, v: TestColumnValue, start: Int) = {
    stmt.setLong(start, v.asInstanceOf[IdValue].value.underlying)
    start + 1
  }

  def sql_in(literals: Iterable[TestColumnValue]) =
    literals.map(_.asInstanceOf[IdValue].value.underlying).mkString("(" + base + " in (", ",", "))")

  def count = "count(" + base + ")"

  def templateForSingleLookup = "(" + base + " = ?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: TestColumnValue, start: Int) =
    prepareMultiLookup(stmt, v, start)

  def sql_==(literal: TestColumnValue) = "(" + base + " = " + literal.asInstanceOf[IdValue].value.underlying + ")"

  def equalityIndexExpression = base
}

class NumberRep(val cid: ColumnId) extends SqlColumnReadRep[TestColumnType, TestColumnValue] {
  def representedType = NumberType

  val base = "c_" + cid.underlying

  def physColumns = Array(base)

  def sqlTypes = Array("BIGINT")

  def fromResultSet(rs: ResultSet, start: Int) = {
    val v = rs.getLong(start)
    if(rs.wasNull) NullValue
    else NumberValue(v)
  }
}

class StringRep(val cid: ColumnId) extends SqlPKableColumnReadRep[TestColumnType, TestColumnValue] {
  def representedType = StringType

  val base = "c_" + cid.underlying

  def physColumns = Array(base)

  def sqlTypes = Array("VARCHAR(255)")

  def fromResultSet(rs: ResultSet, start: Int) = {
    val v = rs.getString(start)
    if(v == null) NullValue
    else StringValue(v)
  }

  def templateForMultiLookup(n: Int) = (1 to n).map(_ => "?").mkString("(" + base + " in (", ",", "))")

  def prepareMultiLookup(stmt: PreparedStatement, v: TestColumnValue, start: Int) = {
    stmt.setString(start, v.asInstanceOf[StringValue].value)
    start + 1
  }

  private def escape(s: String) = {
    val sb = new StringBuilder("'")
    var i = 0
    while(i != s.length) {
      val c = s.charAt(i)
      if(c == '\'') sb.append('\'')
      sb.append(c)
      i += 1
    }
    sb.append('\'')
    sb.toString
  }

  def sql_in(literals: Iterable[TestColumnValue]) =
    literals.map { v => escape(v.asInstanceOf[StringValue].value) }.mkString("(" + base + " in (", ",", "))")

  def count = "count(" + base + ")"

  def templateForSingleLookup = "(" + base + " = ?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: TestColumnValue, start: Int) =
    prepareMultiLookup(stmt, v, start)

  def sql_==(literal: TestColumnValue) = "(" + base + " = " + escape(literal.asInstanceOf[StringValue].value) + ")"

  def equalityIndexExpression = base
}
