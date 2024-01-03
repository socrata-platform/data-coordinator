package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLBoolean, SoQLType}

class BooleanRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  def templateForMultiLookup(n: Int): String =
    s"($base in (${(1 to n).map(_ => "?").mkString(",")}))"

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setBoolean(start, v.asInstanceOf[SoQLBoolean].value)
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
      lit.asInstanceOf[SoQLBoolean].value.toString
    }.mkString(s"($base in (", ",", "))")

  def count: String = s"count($base)"

  def templateForSingleLookup: String = s"($base = ?)"

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def sql_==(literal: SoQLValue): String = {
    val v = literal.asInstanceOf[SoQLBoolean].value.toString
    s"($base = $v)"
  }

  def equalityIndexExpression: String = base

  def representedType: SoQLType = SoQLBoolean

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("BOOLEAN")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SoQLValue): Seq[Option[String]] = {
    if(SoQLNull == v) Seq(None)
    else Seq(Some(if(v.asInstanceOf[SoQLBoolean].value) "true" else "false"))
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.BOOLEAN)
    else stmt.setBoolean(start, v.asInstanceOf[SoQLBoolean].value)
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else 5

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val b = rs.getBoolean(start)
    if(rs.wasNull) SoQLNull
    else SoQLBoolean.canonicalValue(b)
  }
}
