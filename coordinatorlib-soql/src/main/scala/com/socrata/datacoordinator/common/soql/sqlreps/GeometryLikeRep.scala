package com.socrata.datacoordinator.common.soql.sqlreps

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLGeometryLike, SoQLNull, SoQLType, SoQLValue}
import com.vividsolutions.jts.geom.Geometry
import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

class GeometryLikeRep[T<:Geometry](repType: SoQLType, geometry: SoQLValue => T, value: T => SoQLValue, val base: String)
  extends RepUtils with SqlColumnRep[SoQLType, SoQLValue]  {

  def representedType = repType

  def toWkt(v: SoQLValue) = v.typ.asInstanceOf[SoQLGeometryLike[T]].WktRep(geometry(v))

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array("GEOMETRY")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNull == v) { /* pass */ }
    else csvescape(sb, toWkt(v))
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if (SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setString(start, toWkt(v))

    start + 1
  }

  def estimateSize(v: SoQLValue): Int = {
    if (SoQLNull == v) standardNullInsertSize
    else toWkt(v).length
  }

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val v = rs.getString(start)
    if(v == null) SoQLNull

    repType.asInstanceOf[SoQLGeometryLike[T]].JsonRep.unapply(v) match {
      case Some(geometry) => value(geometry)
      case _ => SoQLNull
    }
  }
}
