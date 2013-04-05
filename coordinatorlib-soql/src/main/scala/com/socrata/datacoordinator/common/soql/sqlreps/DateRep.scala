package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import org.joda.time.LocalDate

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLDate, SoQLType}
import org.joda.time.format.ISODateTimeFormat
import com.socrata.datacoordinator.common.soql.SoQLNullValue

class DateRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, Any] {
  val printer = ISODateTimeFormat.date
  val parser = ISODateTimeFormat.dateElementParser

  override def templateForInsert = "(? :: DATE)"

  def templateForMultiLookup(n: Int): String =
    s"($base in (${(1 to n).map(_ => "(? :: DATE)").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: Any, start: Int): Int = {
    stmt.setString(start, printer.print(v.asInstanceOf[LocalDate]))
    start + 1
  }

  def literalize(t: LocalDate) =
    "(DATE '" + printer.print(t) + "')"

  def sql_in(literals: Iterable[Any]): String =
    literals.iterator.map { lit =>
      literalize(lit.asInstanceOf[LocalDate])
    }.mkString(s"($base in (", ",", "))")

  def templateForSingleLookup: String = s"($base = (? :: DATE))"

  def prepareSingleLookup(stmt: PreparedStatement, v: Any, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: Any): String = {
    val v = literalize(literal.asInstanceOf[LocalDate])
    s"($base = $v)"
  }

  def equalityIndexExpression: String = base

  def representedType: SoQLType = SoQLDate

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("DATE")

  def csvifyForInsert(sb: StringBuilder, v: Any) {
    if(SoQLNullValue == v) { /* pass */ }
    else {
      val x = printer.print(v.asInstanceOf[LocalDate])
      sb.append(x)
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: Any, start: Int): Int = {
    if(SoQLNullValue == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setObject(start, printer.print(v.asInstanceOf[LocalDate]), Types.VARCHAR)
    start + 1
  }

  def estimateInsertSize(v: Any): Int =
    if(SoQLNullValue == v) standardNullInsertSize
    else 30

  def SETsForUpdate(sb: StringBuilder, v: Any) {
    sb.append(base).append('=')
    if(SoQLNullValue == v) sb.append("NULL")
    else sb.append(literalize(v.asInstanceOf[LocalDate]))
  }

  def estimateUpdateSize(v: Any): Int =
    base.length + 30

  def fromResultSet(rs: ResultSet, start: Int): Any = {
    val ts = rs.getString(start)
    if(ts == null) SoQLNullValue
    else parser.parseLocalDate(ts)
  }
}
