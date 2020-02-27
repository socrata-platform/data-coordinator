package com.socrata.datacoordinator.common.soql.sqlreps

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLGeometryLike, SoQLNull, SoQLType, SoQLValue}
import com.vividsolutions.jts.geom.Geometry
import java.lang.StringBuilder
import java.sql.{ResultSet, Types, PreparedStatement}

class GeometryLikeRep[T<:Geometry](
  val representedType: SoQLType,
  geometry: SoQLValue => T,
  value: T => SoQLValue,
  val presimplifiedZoomLevels: Seq[Int],
  val base: String)
    extends RepUtils with SqlColumnRep[SoQLType, SoQLValue]  {
  private val WGS84SRID = 4326

  def fromWkt(str: String): Option[T] = representedType.asInstanceOf[SoQLGeometryLike[T]].WktRep.unapply(str)
  def fromWkb(bytes: Array[Byte]): Option[T] = representedType.asInstanceOf[SoQLGeometryLike[T]].WkbRep.unapply(bytes)
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

    new GeometryLikeRep[T](representedType, geometry, value, presimplifiedZoomLevels, newBase) {
      override def forZoom(level: Int) = original.forZoom(level)
    }
  }

  val sqlTypes: Array[String] = Array("GEOMETRY(Geometry," + WGS84SRID + ")")

  override lazy val selectListTransforms = Array(("ST_AsBinary(", ")"))

  override lazy val insertPlaceholders: Array[String] = Array("ST_GeomFromEWKT(?)")

  override def templateForUpdate: String = base + "=ST_GeomFromEWKT(?)"

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    if(SoQLNull == v) { /* pass */ }
    else csvescape(sb, toEWkt(v))
  }

  val prepareInserts = Array(
    { (stmt: PreparedStatement, v: SoQLValue, start: Int) =>
      if(SoQLNull == v) stmt.setNull(start, Types.VARCHAR)
      else stmt.setString(start, toEWkt(v))
    }
  )

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
