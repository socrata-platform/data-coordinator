package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{PreparedStatement, ResultSet, Types}

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.truth.sql.SqlPKableColumnRep
import com.socrata.soql.types._

class DocumentRep(val base: String) extends RepUtils with SqlPKableColumnRep[SoQLType, SoQLValue] {
  def representedType: SoQLType = SoQLDocument

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("JSONB")

  override def templateForUpdate: String = physColumns.map(_ + "=?::JSONB").mkString(",")

  override def templateForInsert: String = physColumns.map(_ => "?::JSONB").mkString(",")

  def templateForSingleLookup: String = s"($base @> (? :: JSONB))"

  def templateForMultiLookup(n: Int): String =
    (1 to n).map { _ => templateForSingleLookup }.mkString("(", " OR ", ")")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SoQLValue) = {
    v match {
      case x: SoQLDocument =>
        Seq(Some(JsonUtil.renderJson(x)))
      case SoQLNull =>
        Seq(None)
      case unknown =>
        throw new Exception("unknown SoQLValue")
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    v match {
      case x: SoQLDocument =>
        stmt.setObject(start, JsonUtil.renderJson(x))
      case SoQLNull =>
        stmt.setNull(start, Types.VARCHAR)
      case unknown =>
        throw new Exception("unknown SoQLValue")
    }
    start + 1
  }

  def prepareMultiLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setString(start, JsonUtil.renderJson(v.asInstanceOf[SoQLDocument]))
    start + 1
  }

  def prepareSingleLookup(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = prepareMultiLookup(stmt, v, start)

  def estimateSize(v: SoQLValue): Int = {
    if (SoQLNull == v) standardNullInsertSize
    else {
      JsonUtil.renderJson(v.asInstanceOf[SoQLDocument]).size
    }
  }

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    Option(rs.getString(start)) match {
      case None => SoQLNull
      case Some(js) =>
        JsonUtil.parseJson[SoQLDocument](js) match {
          case Right(x) => x
          case _ => SoQLNull
        }
    }
  }

  def equalityIndexExpression: String = base

  def sql_==(literal: SoQLValue): String = {
    val v = sqlescape(JsonUtil.renderJson(literal.asInstanceOf[SoQLDocument]))
    s"($base @> ($v :: JSONB))"
  }

  def sql_in(literals: Iterable[SoQLValue]): String =
    literals.iterator.collect {
      case lit: SoQLDocument =>
        sql_==(lit)
    }.mkString("(", " OR ", ")") // Not IN because == on this type is not a simple equality operation


  def count: String = s"count($base)"
}
