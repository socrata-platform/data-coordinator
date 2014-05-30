package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast.{JString, JNull, JValue}
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types._
import com.vividsolutions.jts.geom.Geometry

class GeometryLikeRep[T <: Geometry](repType: SoQLType, geometry: SoQLValue => T, value: T => SoQLValue) extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = repType

  def fromWkt(str: String) = repType.asInstanceOf[SoQLGeometryLike[T]].WktRep.unapply(str)
  def toWkt(v: SoQLValue) = v.typ.asInstanceOf[SoQLGeometryLike[T]].WktRep(geometry(v))

  def fromJValue(input: JValue) = input match {
    case JString(s) => fromWkt(s).map(geometry => value(geometry))
    case JNull => Some(SoQLNull)
    case _ => None
  }

  def toJValue(input: SoQLValue) = {
    if (SoQLNull == input) JNull
    else JString(toWkt(input))
  }
}