package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLLocation, SoQLType}

class LocationRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {
  def representedType: SoQLType = SoQLLocation

  val physColumns: Array[String] = Array(base + "_lat", base + "_lon")

  val sqlTypes: Array[String] = Array("DOUBLE PRECISION", "DOUBLE PRECISION")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNull == v) { sb.append(',') }
    else {
      val ll = v.asInstanceOf[SoQLLocation]
      sb.append(ll.latitude).append(',').append(ll.longitude)
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) {
      stmt.setNull(start, Types.DOUBLE)
      stmt.setNull(start+1, Types.DOUBLE)
    } else {
      val ll = v.asInstanceOf[SoQLLocation]
      stmt.setDouble(start, ll.latitude)
      stmt.setDouble(start + 1, ll.longitude)
    }
    start + 2
  }

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else 40

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val lat = rs.getDouble(start)
    if(rs.wasNull) SoQLNull
    else SoQLLocation(lat, rs.getDouble(start + 1))
  }
}
