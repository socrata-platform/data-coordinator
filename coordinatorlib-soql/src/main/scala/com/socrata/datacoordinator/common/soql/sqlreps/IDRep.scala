package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLType}
import com.socrata.datacoordinator.id.RowId

class IDRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  def templateForMultiLookup(n: Int): String =
    s"($base in (${(1 to n).map(_ => "?").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setLong(start, v.asInstanceOf[SoQLIDValue].underlying)
    start + 1
  }

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map { lit =>
      lit.asInstanceOf[SoQLIDValue].underlying
    }.mkString(s"($base in (", ",", "))")

  def templateForSingleLookup: String = s"($base = ?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val v = literal.asInstanceOf[SoQLIDValue].underlying
    s"($base = $v)"
  }

  def equalityIndexExpression: String = base

  def representedType: SoQLType = SoQLFixedTimestamp

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("BIGINT")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNullValue == v) { /* pass */ }
    else sb.append(v.asInstanceOf[SoQLIDValue].underlying)
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setLong(start, v.asInstanceOf[SoQLIDValue].underlying)
    start + 1
  }

  def estimateInsertSize(v: SoQLValue): Int =
    30

  def SETsForUpdate(sb: StringBuilder, v: SoQLValue) {
    sb.append(base).append('=').append(v.asInstanceOf[SoQLIDValue].underlying)
  }

  def estimateUpdateSize(v: SoQLValue): Int =
    base.length + 30

  def fromResultSet(rs: ResultSet, start: Int): SoQLIDValue =
    SoQLIDValue(new RowId(rs.getLong(start)))

  override def orderBy(ascending: Boolean, nullsFirst: Option[Boolean]) =
    simpleOrderBy(Array(base), ascending, nullsFirst)
}
