package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import org.joda.time.LocalDate

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLDate, SoQLType}
import org.joda.time.format.ISODateTimeFormat

class DateRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  import DateRep._

  def printer = ISODateTimeFormat.date
  def parser = ISODateTimeFormat.localDateParser

  override def templateForInsert: String = placeholder

  def templateForMultiLookup(n: Int): String =
    s"($base in (${Iterator.fill(n)(placeholder).mkString(",")}))"

  override lazy val templateForUpdate = s"$base = $placeholder"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setString(start, printer.print(v.asInstanceOf[SoQLDate].value))
    start + 1
  }

  def literalize(t: LocalDate): StringBuilder = literalizeTo(new StringBuilder, t)

  def literalizeTo(sb: StringBuilder, t: LocalDate): StringBuilder = {
    sb.append('(').append(dateType).append(" '")
    printer.printTo(sb, t)
    sb.append("')")
  }

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map { lit =>
      literalize(lit.asInstanceOf[SoQLDate].value)
    }.mkString(s"($base in (", ",", "))")

  def count: String = s"count($base)"

  def templateForSingleLookup: String = s"($base = $placeholder)"

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

  val sqlTypes: Array[String] = Array(dateType)

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNull == v) { /* pass */ }
    else printer.printTo(sb, v.asInstanceOf[SoQLDate].value)
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setObject(start, printer.print(v.asInstanceOf[SoQLDate].value), Types.VARCHAR)
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else 30

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val ts = rs.getString(start)
    if(ts == null) SoQLNull
    else SoQLDate(parser.parseLocalDate(ts))
  }
}

object DateRep {
  private val dateType = "DATE"
  private val placeholder = s"(? :: $dateType)"
}
