package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLType}

class NumberLikeRep(repType: SoQLType, num: SoQLValue => java.math.BigDecimal, value: java.math.BigDecimal => SoQLValue, val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  def representedType: SoQLType = repType

  def templateForMultiLookup(n: Int): String =
    s"($base in (${(1 to n).map(_ => "?").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setBigDecimal(start, num(v))
    start + 1
  }

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map(num).mkString(s"($base in (", ",", "))")

  def count: String = s"count($base)"

  def templateForSingleLookup: String = s"($base = ?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val v = num(literal)
    s"($base = $v)"
  }

  def equalityIndexExpression: String = base

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("NUMERIC")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SoQLValue) = {
    if(SoQLNull == v) Seq(None)
    else Seq(Some(num(v).toString))
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.DECIMAL)
    else stmt.setBigDecimal(start, num(v))
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else num(v).toString.length //ick

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val b = rs.getBigDecimal(start)
    if(b == null) SoQLNull
    else value(b)
  }
}
