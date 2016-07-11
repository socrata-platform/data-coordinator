package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{PreparedStatement, ResultSet, Types}

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLBlob, SoQLType, SoQLValue}

class BlobRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {
  def representedType: SoQLType = SoQLBlob

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("TEXT")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    if(SoQLNull == v) { /* pass */ }
    else csvescape(sb, v.asInstanceOf[SoQLBlob].value)
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setString(start, v.asInstanceOf[SoQLBlob].value)
    start + 1
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else v.asInstanceOf[SoQLBlob].value.length

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val s = rs.getString(start)
    if(s == null) SoQLNull
    else SoQLBlob(s)
  }
}
