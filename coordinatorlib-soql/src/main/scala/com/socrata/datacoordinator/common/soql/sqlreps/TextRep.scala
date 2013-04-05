package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLText, SoQLType}

class TextRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  def templateForMultiLookup(n: Int): String =
    s"(lower($base) in (${(1 to n).map(_ => "lower(?)").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setString(start, v.asInstanceOf[SoQLTextValue].value)
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
      val escaped = sqlescape(lit.asInstanceOf[SoQLTextValue].value)
      s"lower($escaped)"
    }.mkString(s"(lower($base) in (", ",", "))")

  def templateForSingleLookup: String = s"(lower($base) = lower(?))"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val escaped = sqlescape(literal.asInstanceOf[SoQLTextValue].value)
    s"(lower($base) = lower($escaped))"
  }

  def equalityIndexExpression: String = s"lower($base) text_pattern_ops"

  def representedType: SoQLType = SoQLText

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("TEXT")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNullValue == v) { /* pass */ }
    else csvescape(sb, v.asInstanceOf[SoQLTextValue].value)
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNullValue == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setString(start, v.asInstanceOf[SoQLTextValue].value)
    start + 1
  }

  def estimateInsertSize(v: SoQLValue): Int =
    if(SoQLNullValue == v) standardNullInsertSize
    else v.asInstanceOf[SoQLTextValue].value.length

  def SETsForUpdate(sb: StringBuilder, v: SoQLValue) {
    sb.append(base).append('=')
    if(SoQLNullValue == v) sb.append("NULL")
    else sqlescape(sb, v.asInstanceOf[SoQLTextValue].value)
  }

  def estimateUpdateSize(v: SoQLValue): Int =
    base.length + (if(SoQLNullValue == v) 5 else v.asInstanceOf[SoQLTextValue].value.length)

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val s = rs.getString(start)
    if(s == null) SoQLNullValue
    else SoQLTextValue(s)
  }
}
