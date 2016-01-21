package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{PreparedStatement, ResultSet, Types}

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types._

class PhoneRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {

  def representedType: SoQLType = SoQLPhone

  val physColumns: Array[String] = Array(base + "_number", base + "_type")

  val sqlTypes: Array[String] = Array("TEXT", "TEXT")

  // sub-column offsets
  val numberOffset = 0
  val typeOffset = 1
  val lastOffset = 2

  override def selectList: String =
    Array(physColumns(numberOffset), physColumns(typeOffset)).mkString(",")

  override def templateForUpdate: String = Array(
    s"${physColumns(numberOffset)}=?",
    s"${physColumns(typeOffset)}=?").mkString(",")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    v match {
      case SoQLPhone(phoneNumber, phoneType) =>
        phoneNumber.foreach(csvescape(sb, _))
        sb.append(",")
        phoneType.foreach(x => csvescape(sb, camelCase(x))) // normalize phone type
      case SoQLNull =>
        sb.append(",") // null, null for two sub-columns
      case unknown =>
        throw new Exception("unknown SoQLValue")
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if (SoQLNull == v) {
      stmt.setNull(start + numberOffset, Types.VARCHAR)
      stmt.setNull(start + typeOffset, Types.VARCHAR)
    }
    else {
      val phone = v.asInstanceOf[SoQLPhone]
      phone.phoneNumber match {
        case Some(x) => stmt.setString(start + numberOffset, x)
        case None => stmt.setNull(start, Types.VARCHAR)
      }
      phone.phoneType match {
        case Some(x) => stmt.setString(start + typeOffset, x)
        case None => stmt.setNull(start + typeOffset, Types.VARCHAR)
      }
    }
    start + lastOffset
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else {
      val phone = v.asInstanceOf[SoQLPhone]
      phone.phoneNumber.map(_.length).getOrElse(0) +
        phone.phoneType.map(_.length).getOrElse(0)
    }

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val phoneNumber = Option(rs.getString(start + numberOffset))
    val phoneType = Option(rs.getString(start + typeOffset))
    if (phoneNumber.nonEmpty || phoneType.nonEmpty) {
      SoQLPhone(phoneNumber, phoneType)
    } else {
      SoQLNull
    }
  }

  private def camelCase(s: String): String = s.toLowerCase.capitalize

}
