package com.socrata.datacoordinator.main.soql

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLBoolean, SoQLType}

object BooleanRepFactory extends RepFactory {
  def apply(colBase: String) =
    new SqlPKableColumnRep[SoQLType, Any] {
      def templateForMultiLookup(n: Int): String =
        s"($base in (${(1 to n).map(_ => "?").mkString(",")}))"

      def prepareMultiLookup(stmt: PreparedStatement, v: Any, start: Int): Int = {
        stmt.setBoolean(start, v.asInstanceOf[Boolean])
        start + 1
      }

      /** Generates a SQL expression equivalent to "`column in (literals...)`".
        * @param literals The `StringBuilder` to which to add the data.  Must be non-empty.
        *                 The individual values' types must be equal to (not merely compatible with!)
        *                 `representedType`.
        * @return An expression suitable for splicing into a SQL statement.
        */
      def sql_in(literals: Iterable[Any]): String =
        literals.iterator.map { lit =>
          lit.asInstanceOf[Boolean].toString
        }.mkString(s"($base in (", ",", "))")

      def templateForSingleLookup: String = s"($base = ?)"

      def prepareSingleLookup(stmt: PreparedStatement, v: Any, start: Int): Int = prepareMultiLookup(stmt, v, start)

      def sql_==(literal: Any): String = {
        val v = literal.asInstanceOf[Boolean].toString
        s"($base = $v)"
      }

      def equalityIndexExpression: String = base

      def representedType: SoQLType = SoQLBoolean

      val base: String = colBase

      val physColumns: Array[String] = Array(base)

      val sqlTypes: Array[String] = Array("BOOLEAN")

      def csvifyForInsert(sb: StringBuilder, v: Any) {
        if(v == SoQLNullValue) { /* pass */ }
        else sb.append(v.asInstanceOf[Boolean].toString)
      }

      def prepareInsert(stmt: PreparedStatement, v: Any, start: Int): Int = {
        if(v == SoQLNullValue) stmt.setNull(start, Types.BOOLEAN)
        else stmt.setBoolean(start, v.asInstanceOf[Boolean])
        start + 1
      }

      def estimateInsertSize(v: Any): Int =
        if(v == SoQLNullValue) standardNullInsertSize
        else 5

      def SETsForUpdate(sb: StringBuilder, v: Any) {
        sb.append(base).append('=')
        if(v == SoQLNullValue) sb.append("NULL")
        else sb.append(v.asInstanceOf[Boolean].toString)
      }

      def estimateUpdateSize(v: Any): Int =
        base.length + 11

      def fromResultSet(rs: ResultSet, start: Int): Any = {
        val b = rs.getBoolean(start)
        if(rs.wasNull) SoQLNullValue
        else b
      }
    }
}
