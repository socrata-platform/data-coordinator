package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}
import java.time.format.{DateTimeFormatter => JFormatter}
import java.time.Instant

import org.joda.time.{DateTime, DateTimeZone}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLFixedTimestamp, SoQLType}
import org.joda.time.format.{DateTimeFormatterBuilder, ISODateTimeFormat, DateTimeFormatter}

// This class avoids java.sql.Timestamp because it does _weird_ things
// when dealing with pre-Gregorian dates, which we have a few of.
class FixedTimestampRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  import FixedTimestampRep._

  val SIZE_GUESSTIMATE = 30

  override def selectList =
    // This, sadly, appears to be the best way to get postgresql to
    // produce an ISO8601 timestamp...
    s"(to_json($base)#>>'{}')"

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
    else SoQLFixedTimestamp(new DateTime(FixedTimestampHelper.parse(ts).toEpochMilli))
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
}
