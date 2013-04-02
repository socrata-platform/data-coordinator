package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLID, SoQLFixedTimestamp, SoQLType}
import com.socrata.datacoordinator.id.{RowIdProcessor, RowId}

class IDRep(val base: String, rowIdProcessor: RowIdProcessor) extends RepUtils with SqlPKableColumnRep[SoQLType, Any] {
  val physColumns: Array[String] = Array(base, base + "_obfs")

  val sqlTypes: Array[String] = Array("BIGINT", "CHAR(9)")

  def templateForMultiLookup(n: Int): String =
    s"($base in (${(1 to n).map(_ => "?").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: Any, start: Int): Int = {
    stmt.setLong(start, v.asInstanceOf[RowId].numeric)
    start + 1
  }

  def sql_in(literals: Iterable[Any]): String =
    literals.iterator.map { lit =>
      lit.asInstanceOf[RowId].numeric
    }.mkString(s"($base in (", ",", "))")

  def templateForSingleLookup: String = s"($base = ?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: Any, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: Any): String = {
    val v = literal.asInstanceOf[RowId].numeric
    s"($base = $v)"
  }

  def equalityIndexExpression: String = base

  def representedType: SoQLType = SoQLID

  def csvifyForInsert(sb: StringBuilder, v: Any) {
    if(SoQLNullValue == v) { sb.append(',') }
    else {
      csvescape(sb.append(v.asInstanceOf[RowId].numeric).append(','), v.asInstanceOf[RowId].obfuscated)
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: Any, start: Int): Int = {
    stmt.setLong(start, v.asInstanceOf[RowId].numeric)
    stmt.setString(start + 1, v.asInstanceOf[RowId].obfuscated)
    start + 2
  }

  def estimateInsertSize(v: Any): Int =
    30

  def SETsForUpdate(sb: StringBuilder, v: Any) {
    sqlescape(sb.append(physColumns(0)).append('=').append(v.asInstanceOf[RowId].numeric).append(',').append(physColumns(1)).append('='), v.asInstanceOf[RowId].obfuscated)
  }

  def estimateUpdateSize(v: Any): Int =
    base.length + 30

  def fromResultSet(rs: ResultSet, start: Int): Any = {
    rowIdProcessor(rs.getLong(start), rs.getString(start + 1))
  }
}
