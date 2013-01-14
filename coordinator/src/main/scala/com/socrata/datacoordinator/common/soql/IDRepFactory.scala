package com.socrata.datacoordinator.common.soql

import java.lang.StringBuilder
import java.sql.{ResultSet, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLType}
import com.socrata.datacoordinator.id.RowId

object IDRepFactory extends RepFactory {
  def apply(colBase: String) =
    new SqlPKableColumnRep[SoQLType, Any] {
      def templateForMultiLookup(n: Int): String =
        s"($base in (${(1 to n).map(_ => "?").mkString(",")}))"

      def prepareMultiLookup(stmt: PreparedStatement, v: Any, start: Int): Int = {
        stmt.setLong(start, v.asInstanceOf[RowId].underlying)
        start + 1
      }

      def sql_in(literals: Iterable[Any]): String =
        literals.iterator.map { lit =>
          lit.asInstanceOf[RowId].underlying
        }.mkString(s"($base in (", ",", "))")

      def templateForSingleLookup: String = s"($base = ?)"

      def prepareSingleLookup(stmt: PreparedStatement, v: Any, start: Int): Int = prepareMultiLookup(stmt, v, start)

      def sql_==(literal: Any): String = {
        val v = literal.asInstanceOf[RowId].underlying
        s"($base = $v)"
      }

      def equalityIndexExpression: String = base

      def representedType: SoQLType = SoQLFixedTimestamp

      val base: String = colBase

      val physColumns: Array[String] = Array(base)

      val sqlTypes: Array[String] = Array("BIGINT")

      def csvifyForInsert(sb: StringBuilder, v: Any) {
        if(v == SoQLNullValue) { /* pass */ }
        else sb.append(v.asInstanceOf[RowId].underlying)
      }

      def prepareInsert(stmt: PreparedStatement, v: Any, start: Int): Int = {
        stmt.setLong(start, v.asInstanceOf[RowId].underlying)
        start + 1
      }

      def estimateInsertSize(v: Any): Int =
        30

      def SETsForUpdate(sb: StringBuilder, v: Any) {
        sb.append(base).append('=').append(v.asInstanceOf[RowId].underlying)
      }

      def estimateUpdateSize(v: Any): Int =
        base.length + 30

      def fromResultSet(rs: ResultSet, start: Int): Any = {
        new RowId(rs.getLong(start))
      }
    }
}
