package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLText, SoQLType}

class TextRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  def templateForMultiLookup(n: Int): String =
    s"($base in (${(1 to n).map(_ => "?").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setString(start, v.asInstanceOf[SoQLText].value)
    start + 1
  }

  /** Generates a SQL expression equivalent to "`column in (literals...)`".
    * @param literals The `StringBuilder` to which to add the data.  Must be non-empty.
    *                 The individual values' types must be equal to (not merely compatible with!)
    *                 `representedType`.
    * @return An expression suitable for splicing into a SQL statement.
    */
  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.map { lit =>
      sqlescape(lit.asInstanceOf[SoQLText].value)
    }.mkString(s"($base in (", ",", "))")

  def count: String = s"count($base)"

  def templateForSingleLookup: String = s"($base = ?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val escaped = sqlescape(literal.asInstanceOf[SoQLText].value)
    s"($base = $escaped)"
  }

  def equalityIndexExpression: String = s"$base text_pattern_ops"

  def representedType: SoQLType = SoQLText

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("TEXT")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SoQLValue) = {
    if(SoQLNull == v) Seq(None)
    else Seq(Some(v.asInstanceOf[SoQLText].value))
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setString(start, v.asInstanceOf[SoQLText].value)
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else v.asInstanceOf[SoQLText].value.length

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val s = rs.getString(start)
    if(s == null) SoQLNull
    else SoQLText(s)
  }
}
