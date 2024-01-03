package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types._

class IDRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  val SIZE_GUESSTIMATE = 30

  def templateForMultiLookup(n: Int): String =
    s"($base in (${(1 to n).map(_ => "?").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setLong(start, v.asInstanceOf[SoQLID].value)
    start + 1
  }

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map { lit =>
      lit.asInstanceOf[SoQLID].value
    }.mkString(s"($base in (", ",", "))")

  def count: String = s"count($base)"

  def templateForSingleLookup: String = s"($base = ?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val v = literal.asInstanceOf[SoQLID].value
    s"($base = $v)"
  }

  def equalityIndexExpression: String = base

  def representedType: SoQLType = SoQLID

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("BIGINT")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SoQLValue) = {
    if(SoQLNull == v) Seq(None)
    else Seq(Some(v.asInstanceOf[SoQLID].value.toString))
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setLong(start, v.asInstanceOf[SoQLID].value)
    start + 1
  }

  def estimateSize(v: SoQLValue): Int = SIZE_GUESSTIMATE

  def fromResultSet(rs: ResultSet, start: Int): SoQLID =
    SoQLID(rs.getLong(start))

  override def orderBy(ascending: Boolean, nullsFirst: Option[Boolean]) =
    simpleOrderBy(Array(base), ascending, nullsFirst)
}
