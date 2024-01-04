package com.socrata.datacoordinator.common.soql.sqlreps

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLGeometryLike, SoQLNull, SoQLType, SoQLValue}
import com.vividsolutions.jts.geom.Geometry
import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

class GeometryLikeRep[T<:Geometry](
  repType: SoQLType,
  geometry: SoQLValue => T,
  value: T => SoQLValue,
  val presimplifiedZoomLevels: Seq[Int],
  val base: String)
    extends RepUtils with SqlColumnRep[SoQLType, SoQLValue]  {
  private val WGS84SRID = 4326

  def representedType: SoQLType = repType

  def fromWkt(str: String): Option[T] = repType.asInstanceOf[SoQLGeometryLike[T]].WktRep.unapply(str)
  def fromWkb(bytes: Array[Byte]): Option[T] = repType.asInstanceOf[SoQLGeometryLike[T]].WkbRep.unapply(bytes)
  // TODO: Also write to database  using WKB, would be more efficient
  def toEWkt(v: SoQLValue): String = v.typ.asInstanceOf[SoQLGeometryLike[T]].EWktRep(geometry(v), WGS84SRID)

  val physColumns: Array[String] = Array(base)
  def forZoom(level: Int): GeometryLikeRep[T] = {
    val original = this

    val levels = presimplifiedZoomLevels.filter(_ >= level)

    val newBase = if (levels.isEmpty) {
      base
    } else {
      s"${base}_zoom_${levels.min}"
    }

    new GeometryLikeRep[T](repType, geometry, value, presimplifiedZoomLevels, newBase) {
      override def forZoom(level: Int) = original.forZoom(level)
    }
  }

  val sqlTypes: Array[String] = Array("GEOMETRY(Geometry," + WGS84SRID + ")")

  override def selectList: String = "ST_AsBinary(" + base + ")"

  override def templateForInsert: String = "ST_GeomFromEWKT(?)"

  override def templateForUpdate: String = base + "=ST_GeomFromEWKT(?)"

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    csvescape(sb, csvifyForInsert(v))
  }

  def csvifyForInsert(v: SoQLValue) = {
    if(SoQLNull == v) Seq(None)
    else Seq(Some(toEWkt(v)))
  }

  def prepareInsert(stmt: PreparedStatement, v: SoQLValue, start: Int): Int = {
    if(SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
    else stmt.setString(start, toEWkt(v))

    start + 1
  }

  def estimateSize(v: SoQLValue): Int = {
    if(SoQLNull == v) standardNullInsertSize
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
