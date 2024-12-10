package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import org.joda.time.Period
import org.postgresql.util.PGInterval

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLInterval, SoQLType}
import org.joda.time.format.ISODateTimeFormat

class IntervalRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  import IntervalRep._

  def printer = SoQLInterval.StringRep

  override def templateForInsert: String = placeholder

  def templateForMultiLookup(n: Int): String =
    s"($base in (${Iterator.fill(n)(placeholder).mkString(",")}))"

  override lazy val templateForUpdate = s"$base = $placeholder"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setString(start, printer(v.asInstanceOf[SoQLInterval].value))
    start + 1
  }

  def literalize(t: Period): StringBuilder = literalizeTo(new StringBuilder, t)

  def literalizeTo(sb: StringBuilder, t: Period): StringBuilder = {
    sb.append('(').append(intervalType).append(" '")
    sb.append(printer(t))
    sb.append("')")
  }

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map { lit =>
      literalize(lit.asInstanceOf[SoQLInterval].value)
    }.mkString(s"($base in (", ",", "))")

  def count: String = s"count($base)"

  def templateForSingleLookup: String = s"($base = $placeholder)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val sb = new StringBuilder
    sb.append('(').append(base).append('=')
    literalizeTo(sb, literal.asInstanceOf[SoQLInterval].value)
    sb.append(')').toString
  }

  def equalityIndexExpression: String = base

  def representedType: SoQLType = SoQLInterval

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array(intervalType)

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SoQLValue) = {
    if(SoQLNull == v) Seq(None)
    else {
      val sb = new StringBuilder
      printer.printTo(sb, v.asInstanceOf[SoQLInterval].value)
      Seq(Some(sb.toString))
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.OTHER)
    else {
      val period = v.asInstanceOf[SoQLInterval].value
      stmt.setObject(start, new PGInterval(period.getYears, period.getMonths, period.getWeeks * 7 + period.getDays, period.getHours, period.getMinutes, period.getSeconds.toDouble + period.getMillis / 1000.0))
    }
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else 30

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val pgInterval = rs.getObject(start).asInstanceOf[PGInterval]
    if(pgInterval == null) SoQLNull
    else SoQLInterval(new Period(pgInterval.getYears, pgInterval.getMonths, 0, pgInterval.getDays, pgInterval.getHours, pgInterval.getMinutes, pgInterval.getWholeSeconds, pgInterval.getMicroSeconds / 1000))
  }
}

object IntervalRep {
  private val intervalType = "INTERVAL"
  private val placeholder = s"?"
}
