package com.socrata.datacoordinator.common.soql

import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLLocation, SoQLType}

class LocationRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, Any] {
  def representedType: SoQLType = SoQLLocation

  val physColumns: Array[String] = Array(base + "_lat", base + "_lon")

  val sqlTypes: Array[String] = Array("DOUBLE PRECISION", "DOUBLE PRECISION")

  def csvifyForInsert(sb: StringBuilder, v: Any) {
    if(v == SoQLNullValue) { sb.append(',') }
    else {
      val ll = v.asInstanceOf[(Double,Double)]
      sb.append(ll._1).append(',').append(ll._2)
    }
  }

  def prepareInsert(stmt: PreparedStatement, v: Any, start: Int): Int = {
    if(v == SoQLNullValue) {
      stmt.setNull(start, Types.DOUBLE)
      stmt.setNull(start+1, Types.DOUBLE)
    } else {
      val ll = v.asInstanceOf[(Double, Double)]
      stmt.setDouble(start, ll._1)
      stmt.setDouble(start + 1, ll. _2)
    }
    start + 2
  }

  def estimateInsertSize(v: Any): Int =
    if(v == SoQLNullValue) standardNullInsertSize
    else 40

  def SETsForUpdate(sb: StringBuilder, v: Any) {
    if(v == SoQLNullValue) {
      sb.append(physColumns(0)).append("=NULL,").append(physColumns(1)).append("=NULL")
    } else {
      val ll = v.asInstanceOf[(Double, Double)]
      sb.append(physColumns(0)).append('=').append(ll._1).append(',').append(physColumns(1)).append('=').append(ll._2)
    }
  }

  def estimateUpdateSize(v: Any): Int =
    physColumns(0).length + physColumns(1).length + 40

  def fromResultSet(rs: ResultSet, start: Int): Any = {
    val lat = rs.getDouble(start)
    if(rs.wasNull) SoQLNullValue
    else (lat, rs.getDouble(start + 1))
  }
}
