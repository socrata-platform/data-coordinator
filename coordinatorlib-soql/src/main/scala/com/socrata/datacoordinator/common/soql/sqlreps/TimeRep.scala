package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import org.joda.time.LocalTime

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLTime, SoQLType}
import org.joda.time.format.ISODateTimeFormat

class TimeRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  import TimeRep._

  def printer = ISODateTimeFormat.time
  def parser = ISODateTimeFormat.localTimeParser

  override def templateForInsert = placeholder

  def templateForMultiLookup(n: Int): String =
    s"($base in (${Iterator.fill(n)(placeholder).mkString(",")}))"

  override lazy val templateForUpdate = s"$base = $placeholder"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setString(start, printer.print(v.asInstanceOf[SoQLTime].value))
    start + 1
  }

  def literalize(t: LocalTime) =
    literalizeTo(new StringBuilder, t).toString
  def literalizeTo(sb: StringBuilder, t: LocalTime) = {
    sb.append('(').append(timeType).append(" '")
    printer.printTo(sb, t)
    sb.append("')")
  }

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map { lit =>
      literalize(lit.asInstanceOf[SoQLTime].value)
    }.mkString(s"($base in (", ",", "))")

  def templateForSingleLookup: String = s"($base = $placeholder)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val sb = new StringBuilder
    sb.append('(').append(base).append('=')
    literalizeTo(sb, literal.asInstanceOf[SoQLTime].value)
    sb.append(')').toString
  }

  def equalityIndexExpression: String = base

  def representedType: SoQLType = SoQLTime

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array(timeType)

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNull == v) { /* pass */ }
    else printer.printTo(sb, v.asInstanceOf[SoQLTime].value)
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setObject(start, printer.print(v.asInstanceOf[SoQLTime].value), Types.VARCHAR)
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else 30

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val ts = rs.getString(start)
    if(ts == null) SoQLNull
    else SoQLTime(parser.parseLocalTime(ts))
  }
}

object TimeRep {
  private val timeType = "TIME (3) WITHOUT TIME ZONE"
  private val placeholder = s"(? :: $timeType)"
}
