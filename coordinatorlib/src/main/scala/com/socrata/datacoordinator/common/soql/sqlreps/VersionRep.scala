package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLNull, SoQLVersion}

class VersionRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {
  val SIZE_GUESSTIMATE = 30

  def representedType: SoQLType = SoQLVersion

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("BIGINT")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SoQLValue) = {
    if(SoQLNull == v) Seq(None)
    else Seq(Some(v.asInstanceOf[SoQLVersion].value.toString))
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    stmt.setLong(start, v.asInstanceOf[SoQLVersion].value)
    start + 1
  }

  def estimateSize(v: SoQLValue): Int = SIZE_GUESSTIMATE

  def fromResultSet(rs: ResultSet, start: Int): SoQLVersion =
    SoQLVersion(rs.getLong(start))
}
