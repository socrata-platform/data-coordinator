package com.socrata.datacoordinator.common.soql.sqlreps

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLGeometryLike, SoQLNull, SoQLType, SoQLValue}
import com.vividsolutions.jts.geom.Geometry
import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

class GeometryLikeRep[T<:Geometry](repType: SoQLType, geometry: SoQLValue => T, value: T => SoQLValue, val base: String)
  extends RepUtils with SqlColumnRep[SoQLType, SoQLValue]  {
  private val WGS84SRID = 4326

  def representedType = repType

  def fromWkt(str: String) = repType.asInstanceOf[SoQLGeometryLike[T]].WktRep.unapply(str)
  def fromWkb(bytes: Array[Byte]) = repType.asInstanceOf[SoQLGeometryLike[T]].WkbRep.unapply(bytes)
  // TODO: Also write to database  using WKB, would be more efficient
  def toEWkt(v: SoQLValue) = v.typ.asInstanceOf[SoQLGeometryLike[T]].EWktRep(geometry(v), WGS84SRID)

  val physColumns: Array[String] = Array(base)

  val sqlTypes: Array[String] = Array(s"GEOMETRY(Geometry,$WGS84SRID)")

  override def selectList: String = s"ST_AsBinary($base)"

  override def templateForUpdate: String = s"$base=ST_GeomFromEWKT(?)"

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue) {
    if(SoQLNull == v) { /* pass */ }
    else csvescape(sb, toEWkt(v))
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if (SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setString(start, toEWkt(v))

    start + 1
  }

  def estimateSize(v: SoQLValue): Int = {
    if (SoQLNull == v) standardNullInsertSize
    else toEWkt(v).length
  }

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val data = rs.getBytes(start)
    if(data == null) SoQLNull

    fromWkb(data) match {
      case Some(geometry) => value(geometry)
      case _ => SoQLNull
    }
  }
}
