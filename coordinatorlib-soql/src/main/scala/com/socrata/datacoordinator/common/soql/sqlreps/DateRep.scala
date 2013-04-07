package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import org.joda.time.LocalDate

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLDate, SoQLType}
import org.joda.time.format.ISODateTimeFormat

class DateRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  val printer = ISODateTimeFormat.date
  val parser = ISODateTimeFormat.localDateParser

  override def templateForInsert = "(? :: DATE)"

  def templateForMultiLookup(n: Int): String =
    s"($base in (${(1 to n).map(_ => "(? :: DATE)").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setString(start, printer.print(v.asInstanceOf[SoQLDate].value))
    start + 1
  }

  def literalize(t: LocalDate) =
    literalizeTo(new StringBuilder, t)
  def literalizeTo(sb: StringBuilder, t: LocalDate) = {
    sb.append("(DATE '")
    printer.printTo(sb, t)
    sb.append("')")
  }

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map { lit =>
      literalize(lit.asInstanceOf[SoQLDate].value)
    }.mkString(s"($base in (", ",", "))")

  def templateForSingleLookup: String = s"($base = (? :: DATE))"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val sb = new StringBuilder
    sb.append('(').append(base).append('=')
    literalizeTo(sb, literal.asInstanceOf[SoQLDate].value)
    sb.append(')').toString
  }

  def equalityIndexExpression: String = base

  def representedType: SoQLType = SoQLDate

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("DATE")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNull == v) { /* pass */ }
    else printer.printTo(sb, v.asInstanceOf[SoQLDate].value)
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setObject(start, printer.print(v.asInstanceOf[SoQLDate].value), Types.VARCHAR)
    start + 1
  }

  def estimateInsertSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else 30

  def SETsForUpdate(sb: StringBuilder, v: SoQLValue) {
    sb.append(base).append('=')
    if(SoQLNull == v) sb.append("NULL")
    else literalizeTo(sb, v.asInstanceOf[SoQLDate].value)
  }

  def estimateUpdateSize(v: SoQLValue): Int =
    base.length + 30

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val ts = rs.getString(start)
    if(ts == null) SoQLNull
    else SoQLDate(parser.parseLocalDate(ts))
  }
}
