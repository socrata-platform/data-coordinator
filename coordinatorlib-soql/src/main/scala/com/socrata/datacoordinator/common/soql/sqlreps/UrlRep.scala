package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{PreparedStatement, ResultSet, Types}

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types._

class UrlRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {

  def representedType: SoQLType = SoQLUrl

  val physColumns: Array[String] = Array(base + "_url", base + "_description")

  val sqlTypes: Array[String] = Array("TEXT", "TEXT")

  // sub-column offsets
  val urlOffset = 0
  val descriptionOffset = 1
  val lastOffset = 2

  override def selectList: String =
    Array(physColumns(urlOffset), physColumns(descriptionOffset)).mkString(",")

  override def templateForUpdate: String = Array(
    s"${physColumns(urlOffset)}=?",
    s"${physColumns(descriptionOffset)}=?").mkString(",")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    v match {
      case SoQLUrl(url, description) =>
        url.foreach(csvescape(sb, _))
        sb.append(",")
        description.foreach(x => csvescape(sb, x))
      case SoQLNull =>
        sb.append(",") // null, null for two sub-columns
      case unknown =>
        throw new Exception("unknown SoQLValue")
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if (SoQLNull == v) {
      stmt.setNull(start + urlOffset, Types.VARCHAR)
      stmt.setNull(start + descriptionOffset, Types.VARCHAR)
    }
    else {
      val url = v.asInstanceOf[SoQLUrl]
      url.url match {
        case Some(x) => stmt.setString(start + urlOffset, x)
        case None => stmt.setNull(start, Types.VARCHAR)
      }
      url.description match {
        case Some(x) => stmt.setString(start + descriptionOffset, x)
        case None => stmt.setNull(start + descriptionOffset, Types.VARCHAR)
      }
    }
    start + lastOffset
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else {
      val url = v.asInstanceOf[SoQLUrl]
      url.url.map(_.length).getOrElse(0) + url.description.map(_.length).getOrElse(0)
    }

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val url = Option(rs.getString(start + urlOffset))
    val description = Option(rs.getString(start + descriptionOffset))
    if (url.nonEmpty || description.nonEmpty) {
      SoQLUrl(url, description)
    } else {
      SoQLNull
    }
  }
}
