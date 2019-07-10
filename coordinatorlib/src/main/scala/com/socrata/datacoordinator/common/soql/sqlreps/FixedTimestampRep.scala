package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import org.joda.time.DateTime

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLFixedTimestamp, SoQLType}
import org.joda.time.format.{DateTimeFormatterBuilder, ISODateTimeFormat, DateTimeFormatter}

class FixedTimestampRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  import FixedTimestampRep._

  val SIZE_GUESSTIMATE = 30

  override val templateForInsert = placeholder

  override val templateForUpdate = s"$base = $placeholder"

  def templateForMultiLookup(n: Int): String =
    s"($base in (${Iterator.fill(n)(placeholder).mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setTimestamp(start, new java.sql.Timestamp(v.asInstanceOf[SoQLFixedTimestamp].value.getMillis))
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
    if(SoQLNull == v) stmt.setNull(start, Types.TIMESTAMP)
    else stmt.setTimestamp(start, new java.sql.Timestamp(v.asInstanceOf[SoQLFixedTimestamp].value.getMillis))
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else SIZE_GUESSTIMATE

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val ts = rs.getTimestamp(start)
    if(ts == null) SoQLNull
    else SoQLFixedTimestamp(new DateTime(ts.getTime))
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
