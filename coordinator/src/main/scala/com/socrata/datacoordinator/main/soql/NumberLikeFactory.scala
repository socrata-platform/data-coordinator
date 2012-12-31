package com.socrata.datacoordinator.main.soql

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.SoQLType

class NumberLikeFactory(repType: SoQLType, baseBase: String) extends RepFactory {
  def apply(columnId: ColumnId) =
    new SqlPKableColumnRep[SoQLType, Any] {
      def representedType = repType

      def templateForMultiLookup(n: Int): String =
        s"(lower($base) in (${(1 to n).map(_ => "lower(?)").mkString(",")})"

      def prepareMultiLookup(stmt: PreparedStatement, v: Any, start: Int): Int = {
        stmt.setBigDecimal(start, v.asInstanceOf[BigDecimal].underlying)
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
          lit.asInstanceOf[BigDecimal].toString
        }.mkString(s"($base in (", ",", "))")

      def templateForSingleLookup: String = s"($base = ?)"

      def prepareSingleLookup(stmt: PreparedStatement, v: Any, start: Int): Int = prepareMultiLookup(stmt, v, start)

      def sql_==(literal: Any): String = {
        val v = literal.asInstanceOf[BigDecimal].toString
        s"(lower($base) = $v)"
      }

      def equalityIndexExpression: String = base

      val base: String = baseBase + "_" + columnId.underlying

      val physColumns: Array[String] = Array(base)

      val sqlTypes: Array[String] = Array("NUMERIC")

      def csvifyForInsert(sb: StringBuilder, v: Any) {
        if(v == SoQLNullValue) { /* pass */ }
        else sb.append(v.asInstanceOf[BigDecimal].toString)
      }

      def prepareInsert(stmt: PreparedStatement, v: Any, start: Int): Int = {
        if(v == SoQLNullValue) stmt.setNull(start, Types.DECIMAL)
        else stmt.setBigDecimal(start, v.asInstanceOf[BigDecimal].underlying)
        start + 1
      }

      def estimateInsertSize(v: Any): Int =
        if(v == SoQLNullValue) standardNullInsertSize
        else v.asInstanceOf[BigDecimal].toString.length //ick

      def SETsForUpdate(sb: StringBuilder, v: Any) {
        sb.append(base).append('=')
        if(v == SoQLNullValue) sb.append("NULL")
        else sb.append(v.asInstanceOf[BigDecimal].toString)
      }

      def estimateUpdateSize(v: Any): Int =
        base.length + estimateInsertSize(v)

      def fromResultSet(rs: ResultSet, start: Int): Any = {
        val b = rs.getBigDecimal(start)
        if(b == null) SoQLNullValue
        else BigDecimal(b)
      }
    }
}
