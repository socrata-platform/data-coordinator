package com.socrata.datacoordinator.common.soql

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLText, SoQLType}

object TextRepFactory extends RepFactory {
  def apply(colBase: String) =
    new SqlPKableColumnRep[SoQLType, Any] {
      def templateForMultiLookup(n: Int): String =
        s"(lower($base) in (${(1 to n).map(_ => "lower(?)").mkString(",")}))"

      def prepareMultiLookup(stmt: PreparedStatement, v: Any, start: Int): Int = {
        stmt.setString(start, v.asInstanceOf[String])
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
          val escaped = sqlescape(lit.asInstanceOf[String])
          s"lower($escaped)"
        }.mkString(s"(lower($base) in (", ",", "))")

      def templateForSingleLookup: String = s"(lower($base) = lower(?))"

      def prepareSingleLookup(stmt: PreparedStatement, v: Any, start: Int): Int = prepareMultiLookup(stmt, v, start)

      def sql_==(literal: Any): String = {
        val escaped = sqlescape(literal.asInstanceOf[String])
        s"(lower($base) = lower($escaped))"
      }

      def equalityIndexExpression: String = s"lower($base) text_pattern_ops"

      def representedType: SoQLType = SoQLText

      val base: String = colBase

      val physColumns: Array[String] = Array(base)

      val sqlTypes: Array[String] = Array("TEXT")

      def csvifyForInsert(sb: StringBuilder, v: Any) {
        if(v == SoQLNullValue) { /* pass */ }
        else csvescape(sb, v.asInstanceOf[String])
      }

      def prepareInsert(stmt: PreparedStatement, v: Any, start: Int): Int = {
        if(v == SoQLNullValue) stmt.setNull(start, Types.VARCHAR)
        else stmt.setString(start, v.asInstanceOf[String])
        start + 1
      }

      def estimateInsertSize(v: Any): Int =
        if(v == SoQLNullValue) standardNullInsertSize
        else v.asInstanceOf[String].length

      def SETsForUpdate(sb: StringBuilder, v: Any) {
        sb.append(base).append('=')
        if(v == SoQLNullValue) sb.append("NULL")
        else sqlescape(sb, v.asInstanceOf[String])
      }

      def estimateUpdateSize(v: Any): Int =
        base.length + (if(v == SoQLNullValue) 5 else v.asInstanceOf[String].length)

      def fromResultSet(rs: ResultSet, start: Int): Any = {
        val s = rs.getString(start)
        if(s == null) SoQLNullValue
        else s
      }
    }
}
