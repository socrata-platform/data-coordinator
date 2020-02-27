package com.socrata.datacoordinator.common.soql.sqlreps

import java.lang.StringBuilder
import java.math.BigDecimal
import java.sql.{PreparedStatement, ResultSet, Types}

import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types._

class LocationRep(val base: String) extends RepUtils with SqlColumnRep[SoQLType, SoQLValue] {

  private val WGS84SRID = 4326

  val representedType: SoQLType = SoQLLocation

  val physColumns: Array[String] = Array(base + "_geom", base + "_address")

  val sqlTypes: Array[String] = Array("GEOMETRY(Geometry," + WGS84SRID + ")", "TEXT")

  // sub-column offsets
  val geomOffset = 0
  val addressOffset = 1
  val lastOffset = 2

  override lazy val selectListTransforms =
    Array(("ST_AsBinary(", ")"),
          ("", ""))

  override lazy val insertPlaceholders: Array[String] = Array("ST_GeomFromEWKT(?)", "?")

  override def templateForUpdate: String = Array(
    s"${physColumns(geomOffset)}=ST_GeomFromEWKT(?)",
    s"${physColumns(addressOffset)}=?").mkString(",")

  def csvifyForInsert(sb: StringBuilder, v: SoQLValue): Unit = {
    v match {
      case loc@SoQLLocation(lat, lng, address) =>
        if (lat.isDefined && lng.isDefined) {
          sb.append(toEWkt(loc, WGS84SRID))
        }
        sb.append(",")
        address.foreach(csvescape(sb, _))
      case SoQLNull =>
        sb.append(",") // null, null
      case unknown =>
        throw new Exception("unknown SoQLValue")
    }
  }

  private def toEWkt(location: SoQLLocation, srid: Int): String = {
    require(geomNonEmpty(location))
    s"SRID=$srid;POINT(${location.longitude.get} ${location.latitude.get})" // x (longitude), y (latitude)
  }

  val prepareInserts = Array(
    { (stmt: PreparedStatement, v: SoQLValue, start: Int) =>
      if (SoQLNull == v) {
        stmt.setNull(start, Types.VARCHAR)
      } else {
        val loc = v.asInstanceOf[SoQLLocation]
        if (geomNonEmpty(loc)) {
          stmt.setString(start, toEWkt(loc, WGS84SRID))
        } else {
          stmt.setNull(start, Types.VARCHAR)
        }
      }
    },
    { (stmt: PreparedStatement, v: SoQLValue, start: Int) =>
      if (SoQLNull == v) {
        stmt.setNull(start, Types.VARCHAR)
      } else {
        val loc = v.asInstanceOf[SoQLLocation]
        loc.address match {
          case Some(a) => stmt.setString(start, a)
          case None => stmt.setNull(start, Types.VARCHAR)
        }
      }
    }
  )

  def estimateSize(v: SoQLValue): Int =
    if(SoQLNull == v) standardNullInsertSize
    else {
      val loc = v.asInstanceOf[SoQLLocation]
      (if (geomNonEmpty(loc)) toEWkt(loc, WGS84SRID).length else 0) +
        loc.address.map(_.length).getOrElse(0)
    }

  private def geomNonEmpty(v: SoQLLocation) = v.latitude.isDefined && v.longitude.isDefined

  def fromResultSet(rs: ResultSet, start: Int): SoQLValue = {
    val pointWkb = rs.getBytes(start + geomOffset)
    val pointLoc = SoQLPoint.WkbRep.unapply(pointWkb) match {
      case Some(point) =>
        SoQLLocation(Some(BigDecimal.valueOf(point.getY)),
                     Some(BigDecimal.valueOf(point.getX)), None)
      case None => SoQLNull
        SoQLLocation(None, None, None)
    }

    val loc = pointLoc.copy(address = Option(rs.getString(start + addressOffset)))
    if (geomNonEmpty(loc) || loc.address.isDefined) loc
    else SoQLNull
  }
}
