package com.socrata.datacoordinator
package truth.loader.sql

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import java.sql.{Types, PreparedStatement, ResultSet}
import java.lang.StringBuilder

abstract class TestColumnRep(val columnId: ColumnId) extends SqlPKableColumnRep[TestColumnType, TestColumnValue] {
  def templateForMultiLookup(n: Int) = {
    val sb = new StringBuilder("(").append(base).append(" IN (?")
    var i = 1
    while(i < n) {
      sb.append(",?")
      i += 1
    }
    sb.append("))")
    sb.toString
  }

  def templateForSingleLookup = "(" + base + "=?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: TestColumnValue, start: Int) = prepareMultiLookup(stmt, v, start)

  def equalityIndexExpression = base

  val base = "c_" + columnId

  val physColumns = Array(base)
}

class LongRep(columnId: ColumnId) extends TestColumnRep(columnId) {
  def prepareMultiLookup(stmt: PreparedStatement, v: TestColumnValue, start: Int) = {
    stmt.setLong(start, v.asInstanceOf[LongValue].value)
    start + 1
  }

  def sql_in(literals: Iterable[TestColumnValue]) = {
    val sb = new StringBuilder("(").append(base).append(" IN (")
    val it = literals.iterator
    sb.append(it.next())
    while(it.hasNext) {
      sb.append(',').append(it.next().asInstanceOf[LongValue].value)
    }
    sb.append("))")
    sb.toString
  }

  def sql_==(literal: TestColumnValue) =
    "(" + base + "=" + literal.asInstanceOf[LongValue].value + ")"

  def representedType = LongColumn

  val sqlTypes = Array("BIGINT")

  def csvifyForInsert(sb: StringBuilder, v: TestColumnValue) {
    if(v != NullValue) sb.append(v.asInstanceOf[LongValue].value)
  }

  def prepareInsert(stmt: PreparedStatement, v: TestColumnValue, start: Int) = {
    if(v == NullValue) stmt.setNull(start, Types.BIGINT)
    else stmt.setLong(start, v.asInstanceOf[LongValue].value)
    start + 1
  }

  def estimateInsertSize(v: TestColumnValue) = 8

  def SETsForUpdate(sb: StringBuilder, v: TestColumnValue) {
    sb.append(base).append('=')
    if(v == NullValue) sb.append("NULL")
    else sb.append(v.asInstanceOf[LongValue].value)
  }

  def estimateUpdateSize(v: TestColumnValue) = base.length + 8

  def fromResultSet(rs: ResultSet, start: Int) = {
    val l = rs.getLong(start)
    if(rs.wasNull) NullValue
    else LongValue(l)
  }
}

class StringRep(columnId: ColumnId) extends TestColumnRep(columnId) {
  def prepareMultiLookup(stmt: PreparedStatement, v: TestColumnValue, start: Int) = {
    stmt.setString(start, v.asInstanceOf[StringValue].value)
    start + 1
  }

  def sql_in(literals: Iterable[TestColumnValue]) = {
    val sb = new StringBuilder("(").append(base).append(" IN (")
    val it = literals.iterator
    sb.append(it.next())
    while(it.hasNext) {
      sb.append(',').append('\'', escape('\'', it.next().asInstanceOf[StringValue].value))
    }
    sb.append("))")
    sb.toString
  }

  def sql_==(literal: TestColumnValue) =
    "(" + base + "=" + escape('\'', literal.asInstanceOf[StringValue].value) + ")"

  def representedType = StringColumn

  val sqlTypes = Array("VARCHAR(255)")

  def csvifyForInsert(sb: StringBuilder, v: TestColumnValue) {
    if(v != NullValue) sb.append(escape('"', v.asInstanceOf[StringValue].value))
  }

  def prepareInsert(stmt: PreparedStatement, v: TestColumnValue, start: Int) = {
    if(v == NullValue) stmt.setNull(start, Types.BIGINT)
    else stmt.setString(start, v.asInstanceOf[StringValue].value)
    start + 1
  }

  def estimateInsertSize(v: TestColumnValue) =
    if(v == NullValue) 8
    else v.asInstanceOf[StringValue].value.length

  def SETsForUpdate(sb: StringBuilder, v: TestColumnValue) {
    sb.append(base).append('=')
    if(v == NullValue) sb.append("NULL")
    else sb.append(escape('\'', v.asInstanceOf[StringValue].value))
  }

  def estimateUpdateSize(v: TestColumnValue) =
    if(v == NullValue) base.length + 5
    else base.length + v.asInstanceOf[StringValue].value.length

  def fromResultSet(rs: ResultSet, start: Int) = {
    val s = rs.getString(start)
    if(s eq null) NullValue
    else StringValue(s)
  }

  def escape(q: Char, s: String): String = {
    val sb = new StringBuilder
    sb.append(q)
    var i = 0
    while(i != s.length) {
      val c = s.charAt(i)
      if(c == q) sb.append(c)
      sb.append(c)
      i += 1
    }
    sb.append(q)
    sb.toString
  }
}
