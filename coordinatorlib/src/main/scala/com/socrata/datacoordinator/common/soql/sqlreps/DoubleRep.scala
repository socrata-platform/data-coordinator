package com.socrata.datacoordinator.common.soql.sqlreps

import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLDouble, SoQLType}
import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import java.sql.{ResultSet, Types, PreparedStatement}
import java.lang.StringBuilder

class DoubleRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLDouble

  def templateForMultiLookup(n: Int): String =
    s"($base in (${(1 to n).map(_ => "?").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.DOUBLE)
    else stmt.setDouble(start, v.asInstanceOf[SoQLDouble].value)
    start + 1
  }

  def dbl(v: SoQLValue): Double = v.asInstanceOf[SoQLDouble].value

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map(dbl).mkString(s"($base in (", ",", "))")

  def count: String = s"count($base)"

  def templateForSingleLookup: String = s"($base = ?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val v = dbl(literal)
    s"($base = $v)"
  }

  def equalityIndexExpression: String = base

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("DOUBLE PRECISION")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    if(SoQLNull == v) { /* pass */ }
    else sb.append(dbl(v))
  }

  val prepareInserts = Array(
    { (stmt: PreparedStatement, v: SoQLValue, start: Int) =>
      if(SoQLNull == v) stmt.setNull(start, Types.DOUBLE)
      else stmt.setDouble(start, dbl(v))
    }
  )

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else dbl(v).toString.length //ick

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val v = rs.getDouble(start)
    if(rs.wasNull) SoQLNull
    else SoQLDouble(v)
  }
}
