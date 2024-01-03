package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{PreparedStatement, ResultSet, Types}

import com.rojoma.json.v3.ast.{JNull, JValue}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types._

class JsonRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {
  def representedType: SoQLType = SoQLJson

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("JSONB")

  override def templateForUpdate: String = physColumns.map(_ + "=?::JSONB").mkString(",")

  override def templateForInsert: String = physColumns.map(_ => "?::JSONB").mkString(",")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SoQLValue) = {
    v match {
      case SoQLJson(x) =>
        Seq(Some(JsonUtil.renderJson(x)))
      case SoQLNull =>
        Seq(None)
      case unknown =>
        throw new Exception("unknown SoQLValue")
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    v match {
      case SoQLJson(x) =>
        stmt.setObject(start, JsonUtil.renderJson(x))
      case SoQLNull =>
        stmt.setNull(start, Types.VARCHAR)
      case unknown =>
        throw new Exception("unknown SoQLValue")
    }
    start + 1
  }

  def estimateSize(v: SoQLValue): Int = {
    v match {
      case SoQLNull => standardNullInsertSize
      case SoQLJson(js) => JsonUtil.renderJson(js).size
      case _ => ???
    }
  }

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    Option(rs.getString(start)) match {
      case None => SoQLJson(JNull)
      case Some(js) =>
        JsonUtil.parseJson[JValue](js) match {
          case Right(jv) => SoQLJson(jv)
          case _ => SoQLNull // not sure about this.
        }
    }
  }
}
