package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types._
import com.vividsolutions.jts.geom.Geometry

class GeometryLikeRep[T <: Geometry](repType: SoQLType, geometry: SoQLValue => T, value: T => SoQLValue) extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = repType

  def fromWkt(str: String): Option[T] = repType.asInstanceOf[SoQLGeometryLike[T]].WktRep.unapply(str)
  def fromWkb64(str: String): Option[T] = repType.asInstanceOf[SoQLGeometryLike[T]].Wkb64Rep.unapply(str)
  def toWkb64(v: SoQLValue): String = v.typ.asInstanceOf[SoQLGeometryLike[T]].Wkb64Rep(geometry(v))

  def fromJValue(input: JValue): Option[SoQLValue] = input match {
    // Fall back to WKT in case we deal with an old PG-soql-server still outputting WKT
    case JString(s) => fromWkb64(s).orElse(fromWkt(s)).map(geometry => value(geometry))
    case JNull => Some(SoQLNull)
    case _ => None
  }

  def toJValue(input: SoQLValue): JValue = {
    if(SoQLNull == input) JNull
    else JString(toWkb64(input))
  }
}
