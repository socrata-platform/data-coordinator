package com.socrata.datacoordinator.main.soql

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import org.joda.time.DateTime

import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLType}
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormatter}

object FixedTimestampRepFactory extends RepFactory {
  val literalFormatter: DateTimeFormatter = ISODateTimeFormat.dateTime

  def apply(colBase: String) =
    new SqlPKableColumnRep[SoQLType, Any] {
      def templateForMultiLookup(n: Int): String =
        s"($base in (${(1 to n).map(_ => "?").mkString(",")}))"

      def prepareMultiLookup(stmt: PreparedStatement, v: Any, start: Int): Int = {
        stmt.setTimestamp(start, new java.sql.Timestamp(v.asInstanceOf[DateTime].getMillis))
        start + 1
      }

      def literalize(t: DateTime) =
        "('" + literalFormatter.print(t) + "'::TIMESTAMP WITH TIME ZONE)"

      def sql_in(literals: Iterable[Any]): String =
        literals.iterator.map { lit =>
          literalize(lit.asInstanceOf[DateTime])
        }.mkString(s"($base in (", ",", "))")

      def templateForSingleLookup: String = s"($base = ?)"

      def prepareSingleLookup(stmt: PreparedStatement, v: Any, start: Int): Int = prepareMultiLookup(stmt, v, start)

      def sql_==(literal: Any): String = {
        val v = literalize(literal.asInstanceOf[DateTime])
        s"($base = $v)"
      }

      def equalityIndexExpression: String = base

      def representedType: SoQLType = SoQLFixedTimestamp

      val base: String = colBase

      val physColumns: Array[String] = Array(base)

      val sqlTypes: Array[String] = Array("TIMESTAMP WITH TIME ZONE")

      def csvifyForInsert(sb: StringBuilder, v: Any) {
        if(v == SoQLNullValue) { /* pass */ }
        else sb.append(literalFormatter.print(v.asInstanceOf[DateTime]))
      }

      def prepareInsert(stmt: PreparedStatement, v: Any, start: Int): Int = {
        if(v == SoQLNullValue) stmt.setNull(start, Types.TIMESTAMP)
        else stmt.setTimestamp(start, new java.sql.Timestamp(v.asInstanceOf[DateTime].getMillis))
        start + 1
      }

      def estimateInsertSize(v: Any): Int =
        if(v == SoQLNullValue) standardNullInsertSize
        else 30

      def SETsForUpdate(sb: StringBuilder, v: Any) {
        sb.append(base).append('=')
        if(v == SoQLNullValue) sb.append("NULL")
        else sb.append(literalize(v.asInstanceOf[DateTime]))
      }

      def estimateUpdateSize(v: Any): Int =
        base.length + 30

      def fromResultSet(rs: ResultSet, start: Int): Any = {
        val ts = rs.getTimestamp(start)
        if(ts == null) SoQLNullValue
        else new DateTime(ts.getTime)
      }
    }
}
