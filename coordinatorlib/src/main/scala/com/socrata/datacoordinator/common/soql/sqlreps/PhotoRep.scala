package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{PreparedStatement, ResultSet, Types}

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLPhoto, SoQLType, SoQLValue}

class PhotoRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {
  def representedType: SoQLType = SoQLPhoto

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("TEXT")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    if(SoQLNull == v) { /* pass */ }
    else csvescape(sb, v.asInstanceOf[SoQLPhoto].value)
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setString(start, v.asInstanceOf[SoQLPhoto].value)
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else v.asInstanceOf[SoQLPhoto].value.length

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val s = rs.getString(start)
    if(s == null) SoQLNull
    else SoQLPhoto(s)
  }
}
