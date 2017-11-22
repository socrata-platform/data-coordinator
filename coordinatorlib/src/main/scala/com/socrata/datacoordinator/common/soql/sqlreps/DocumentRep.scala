package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{PreparedStatement, ResultSet, Types}

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types._

class DocumentRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {
  def representedType: SoQLType = SoQLDocument

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("JSON")

  override def templateForUpdate: String = physColumns.map(_ + "=?::JSON").mkString(",")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    v match {
      case x: SoQLDocument =>
        csvescape(sb, JsonUtil.renderJson(x))
      case SoQLNull =>
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

  def count: String = s"count($base)"
}
