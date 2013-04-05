package com.socrata.datacoordinator.common.soql
package sqlreps

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLLocation, SoQLType}

class LocationRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {
  def representedType: SoQLType = SoQLLocation

  val physColumns: Array[String] = Array(base + "_lat", base + "_lon")

  val sqlTypes: Array[String] = Array("DOUBLE PRECISION", "DOUBLE PRECISION")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNullValue == v) { sb.append(',') }
    else {
      val ll = v.asInstanceOf[SoQLLocationValue]
      sb.append(ll.latitude).append(',').append(ll.longitude)
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNullValue == v) {
      stmt.setNull(start, Types.DOUBLE)
      stmt.setNull(start+1, Types.DOUBLE)
    } else {
      val ll = v.asInstanceOf[SoQLLocationValue]
      stmt.setDouble(start, ll.latitude)
      stmt.setDouble(start + 1, ll.longitude)
    }
    start + 2
  }

  def estimateInsertSize(v: SoQLValue): Int =
    if(SoQLNullValue == v) standardNullInsertSize
    else 40

  def SETsForUpdate(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNullValue == v) {
      sb.append(physColumns(0)).append("=NULL,").append(physColumns(1)).append("=NULL")
    } else {
      val ll = v.asInstanceOf[SoQLLocationValue]
      sb.append(physColumns(0)).append('=').append(ll.latitude).append(',').append(physColumns(1)).append('=').append(ll.longitude)
    }
  }

  def estimateUpdateSize(v: SoQLValue): Int =
    physColumns(0).length + physColumns(1).length + 40

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val lat = rs.getDouble(start)
    if(rs.wasNull) SoQLNullValue
    else SoQLLocationValue(lat, rs.getDouble(start + 1))
  }
}
