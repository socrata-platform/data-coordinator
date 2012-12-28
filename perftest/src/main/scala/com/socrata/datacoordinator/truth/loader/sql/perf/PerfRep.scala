package com.socrata.datacoordinator
package truth.loader.sql
package perf

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import java.sql.{Types, PreparedStatement, ResultSet}
import java.lang.StringBuilder
import com.socrata.datacoordinator.id.{ColumnId, RowId}

abstract class PerfRep(val columnId: ColumnId) extends SqlPKableColumnRep[PerfType, PerfValue] {
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

  def prepareSingleLookup(stmt: PreparedStatement, v: PerfValue, start: Int) = prepareMultiLookup(stmt, v, start)

  def equalityIndexExpression = base

  val base = "c_" + columnId.underlying

  val physColumns = Array(base)
}

class IdRep(columnId: ColumnId) extends PerfRep(columnId) {
  def prepareMultiLookup(stmt: PreparedStatement, v: PerfValue, start: Int) = {
    stmt.setLong(start, v.asInstanceOf[PVId].value.underlying)
    start + 1
  }

  def sql_in(literals: Iterable[PerfValue]) = {
    val sb = new StringBuilder("(").append(base).append(" IN (")
    val it = literals.iterator
    sb.append(it.next())
    while(it.hasNext) {
      sb.append(',').append(it.next().asInstanceOf[PVId].value.underlying)
    }
    sb.append("))")
    sb.toString
  }

  def sql_==(literal: PerfValue) =
    "(" + base + "=" + literal.asInstanceOf[PVId].value.underlying + ")"

  def representedType = PTId

  val sqlTypes = Array("BIGINT")

  def csvifyForInsert(sb: StringBuilder, v: PerfValue) {
    sb.append(v.asInstanceOf[PVId].value.underlying)
  }

  def prepareInsert(stmt: PreparedStatement, v: PerfValue, start: Int) = {
    stmt.setLong(start, v.asInstanceOf[PVId].value.underlying)
    start + 1
  }

  def estimateInsertSize(v: PerfValue) = 8

  def SETsForUpdate(sb: StringBuilder, v: PerfValue) {
    sb.append(base).append('=').append(v.asInstanceOf[PVId].value.underlying)
  }

  def estimateUpdateSize(v: PerfValue) = base.length + 8

  def fromResultSet(rs: ResultSet, start: Int) =
    PVId(new RowId(rs.getLong(start)))
}

class NumberRep(columnId: ColumnId) extends PerfRep(columnId) {
  def prepareMultiLookup(stmt: PreparedStatement, v: PerfValue, start: Int) = {
    stmt.setBigDecimal(start, v.asInstanceOf[PVNumber].value.underlying)
    start + 1
  }

  def sql_in(literals: Iterable[PerfValue]) = {
    val sb = new StringBuilder("(").append(base).append(" IN (")
    val it = literals.iterator
    sb.append(it.next())
    while(it.hasNext) {
      sb.append(',').append(it.next().asInstanceOf[PVNumber].value)
    }
    sb.append("))")
    sb.toString
  }

  def sql_==(literal: PerfValue) =
    "(" + base + "=" + literal.asInstanceOf[PVNumber].value + ")"

  def representedType = PTNumber

  val sqlTypes = Array("NUMERIC")

  def csvifyForInsert(sb: StringBuilder, v: PerfValue) {
    if(v != PVNull) sb.append(v.asInstanceOf[PVNumber].value)
  }

  def prepareInsert(stmt: PreparedStatement, v: PerfValue, start: Int) = {
    if(v == PVNull) stmt.setNull(start, Types.NUMERIC)
    else stmt.setBigDecimal(start, v.asInstanceOf[PVNumber].value.underlying)
    start + 1
  }

  def estimateInsertSize(v: PerfValue) = if(v == PVNull) 8 else v.asInstanceOf[PVNumber].value.underlying.toString.length

  def SETsForUpdate(sb: StringBuilder, v: PerfValue) {
    sb.append(base).append('=')
    if(v == PVNull) sb.append("NULL")
    else sb.append(v.asInstanceOf[PVNumber].value)
  }

  def estimateUpdateSize(v: PerfValue) =
    if(v == PVNull) base.length + 5
    else base.length + v.asInstanceOf[PVNumber].value.toString.length

  def fromResultSet(rs: ResultSet, start: Int) = {
    val l = rs.getBigDecimal(start)
    if(l == null) PVNull
    else PVNumber(l)
  }
}

class TextRep(columnId: ColumnId) extends PerfRep(columnId) {
  def prepareMultiLookup(stmt: PreparedStatement, v: PerfValue, start: Int) = {
    stmt.setString(start, v.asInstanceOf[PVText].value)
    start + 1
  }

  def sql_in(literals: Iterable[PerfValue]) = {
    val sb = new StringBuilder("(").append(base).append(" IN (")
    val it = literals.iterator
    sb.append(it.next())
    while(it.hasNext) {
      sb.append(',').append('\'', escape('\'', it.next().asInstanceOf[PVText].value))
    }
    sb.append("))")
    sb.toString
  }

  def sql_==(literal: PerfValue) =
    "(" + base + "=" + escape('\'', literal.asInstanceOf[PVText].value) + ")"

  def representedType = PTText

  val sqlTypes = Array("TEXT")

  def csvifyForInsert(sb: StringBuilder, v: PerfValue) {
    if(v != PVNull) sb.append(escape('"', v.asInstanceOf[PVText].value))
  }

  def prepareInsert(stmt: PreparedStatement, v: PerfValue, start: Int) = {
    if(v == PVNull) stmt.setNull(start, Types.VARCHAR)
    else stmt.setString(start, v.asInstanceOf[PVText].value)
    start + 1
  }

  def estimateInsertSize(v: PerfValue) =
    if(v == PVNull) 8
    else v.asInstanceOf[PVText].value.length

  def SETsForUpdate(sb: StringBuilder, v: PerfValue) {
    sb.append(base).append('=')
    if(v == PVNull) sb.append("NULL")
    else sb.append(escape('\'', v.asInstanceOf[PVText].value))
  }

  def estimateUpdateSize(v: PerfValue) =
    if(v == PVNull) base.length + 5
    else base.length + v.asInstanceOf[PVText].value.length

  def fromResultSet(rs: ResultSet, start: Int) = {
    val s = rs.getString(start)
    if(s eq null) PVNull
    else PVText(s)
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
