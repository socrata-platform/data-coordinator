package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}
import java.time.format.{DateTimeFormatter => JFormatter}
import java.time.{Instant, LocalTime, LocalDate, LocalDateTime, ZoneOffset}
import java.util.regex.Pattern

import com.rojoma.json.v3.ast.JString
import org.joda.time.{DateTime, DateTimeZone}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLFixedTimestamp, SoQLType}
import org.joda.time.format.{DateTimeFormatterBuilder, ISODateTimeFormat, DateTimeFormatter}

// This class avoids java.sql.Timestamp because it does _weird_ things
// when dealing with pre-Gregorian dates, which we have a few of.
class FixedTimestampRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  import FixedTimestampRep._

  val SIZE_GUESSTIMATE = 30

  override val templateForInsert = placeholder

  override val templateForUpdate = s"$base = $placeholder"

  def templateForMultiLookup(n: Int): String =
    s"($base in (${Iterator.fill(n)(placeholder).mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setString(start, printer.print(v.asInstanceOf[SoQLFixedTimestamp].value))
    start + 1
  }

  def literalize(t: DateTime): String = {
    val sb = new StringBuilder
    literalizeTo(sb, t)
    sb.toString
  }

  def literalizeTo(sb: StringBuilder, t: DateTime): StringBuilder = {
    sb.append('(').append(timestampType).append(" '")
    printer.printTo(sb, t)
    sb.append("')")
  }

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map { lit =>
      literalize(lit.asInstanceOf[SoQLFixedTimestamp].value)
    }.mkString(s"($base in (", ",", "))")

  def count: String = s"count($base)"

  def templateForSingleLookup: String = s"($base = $placeholder)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val sb = new StringBuilder
    sb.append('(').append(base).append('=')
    literalizeTo(sb, literal.asInstanceOf[SoQLFixedTimestamp].value)
    sb.append(')').toString
  }

  def equalityIndexExpression: String = base

  def representedType: SoQLType = SoQLFixedTimestamp

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array(timestampType)

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNull == v) { /* pass */ }
    else printer.printTo(sb, v.asInstanceOf[SoQLFixedTimestamp].value)
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setString(start, printer.print(v.asInstanceOf[SoQLFixedTimestamp].value))
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else SIZE_GUESSTIMATE

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val ts = rs.getString(start)
    if(ts == null) SoQLNull
    else SoQLFixedTimestamp(new DateTime(parse(ts).toEpochMilli))
  }
}

object FixedTimestampRep {
  private val printer = new DateTimeFormatterBuilder().
    append(ISODateTimeFormat.date).
    appendLiteral(' ').
    append(ISODateTimeFormat.time).
    toFormatter.withZoneUTC

  private val timestampType = "TIMESTAMP (3) WITH TIME ZONE"

  private val placeholder = s"(? :: $timestampType)"

  // Postgresql will hand dates to us formatted like
  //   YYYY[Y...]-MM-DD HH:MM:SS[.ssss...]±HH[:MM[:SS]][ BC]
  // This regex pulls those bits out and then
  // we'll turn them into an Instant.
  private val pattern = Pattern.compile("^([0-9]{4,})-([0-9]{2})-([0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2}(?:.[0-9]+)?)([+-][0-9]{2}(?::[0-9]{2}(?::[0-9]{2})?)?)( BC)?$");

  private def parse(s: String): Instant = {
    val m = pattern.matcher(s)
    if(!m.matches()) throw new IllegalArgumentException("Malformed timestamp: " + JString(s))

    val year = m.group(1)
    val month = m.group(2)
    val day = m.group(3)
    val time = m.group(4)
    val offset = m.group(5)
    val bc = m.group(6) != null

    LocalDateTime.of(LocalDate.of((if(bc) -1 else 1) * year.toInt,
                                  month.toInt,
                                  day.toInt),
                     LocalTime.parse(time)).
      atOffset(ZoneOffset.of(offset)).
      toInstant
  }
}
