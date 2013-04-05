package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import org.joda.time.LocalDateTime

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLFloatingTimestamp, SoQLType}
import org.joda.time.format.{DateTimeFormatterBuilder, ISODateTimeFormat}
import com.socrata.datacoordinator.common.soql.{SoQLFloatingTimestampValue, SoQLValue, SoQLNullValue}

class FloatingTimestampRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  val printer = new DateTimeFormatterBuilder().
    append(ISODateTimeFormat.date).
    appendLiteral(' ').
    append(ISODateTimeFormat.time).
    toFormatter
  val parser = new DateTimeFormatterBuilder().
    append(ISODateTimeFormat.dateElementParser).
    appendLiteral(' ').
    append(ISODateTimeFormat.timeElementParser).
    toFormatter

  override def templateForInsert = "(? :: TIMESTAMP WITHOUT TIME ZONE)"

  def templateForMultiLookup(n: Int): String =
    s"($base in (${(1 to n).map(_ => "(? :: TIMESTAMP WITHOUT TIME ZONE)").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setString(start, printer.print(v.asInstanceOf[SoQLFloatingTimestampValue].value))
    start + 1
  }

  def literalize(t: LocalDateTime) =
    "(TIMESTAMP WITHOUT TIME ZONE '" + printer.print(t) + "')"

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map { lit =>
      literalize(lit.asInstanceOf[SoQLFloatingTimestampValue].value)
    }.mkString(s"($base in (", ",", "))")

  def templateForSingleLookup: String = s"($base = (? :: TIMESTAMP WITHOUT TIME ZONE))"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val v = literalize(literal.asInstanceOf[SoQLFloatingTimestampValue].value)
    s"($base = $v)"
  }

  def equalityIndexExpression: String = base

  def representedType: SoQLType = SoQLFloatingTimestamp

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("TIMESTAMP WITHOUT TIME ZONE")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNullValue == v) { /* pass */ }
    else {
      val x = printer.print(v.asInstanceOf[SoQLFloatingTimestampValue].value)
      sb.append(x)
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNullValue == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setObject(start, printer.print(v.asInstanceOf[SoQLFloatingTimestampValue].value), Types.VARCHAR)
    start + 1
  }

  def estimateInsertSize(v: SoQLValue): Int =
    if(SoQLNullValue == v) standardNullInsertSize
    else 30

  def SETsForUpdate(sb: StringBuilder, v: SoQLValue) {
    sb.append(base).append('=')
    if(SoQLNullValue == v) sb.append("NULL")
    else sb.append(literalize(v.asInstanceOf[SoQLFloatingTimestampValue].value))
  }

  def estimateUpdateSize(v: SoQLValue): Int =
    base.length + 30

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val ts = rs.getString(start)
    if(ts == null) SoQLNullValue
    else SoQLFloatingTimestampValue(parser.parseLocalDateTime(ts))
  }
}
