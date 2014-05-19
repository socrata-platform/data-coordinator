package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast.{JNull, JValue}
import com.rojoma.json.io.{CompactJsonWriter, JsonReader}
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLGeometryLike, SoQLNull, SoQLType, SoQLValue}
import com.vividsolutions.jts.geom.Geometry

class GeometryLikeRep[T <: Geometry](repType: SoQLType, geometry: SoQLValue => T, value: T => SoQLValue) extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = repType

  def fromJson(str: String) = repType.asInstanceOf[SoQLGeometryLike[T]].JsonRep.unapply(str)
  def toJson(v: SoQLValue) = v.typ.asInstanceOf[SoQLGeometryLike[T]].JsonRep(geometry(v))

  def fromJValue(input: JValue) = {
    if (JNull == input) Some(SoQLNull)
    else {
      val geometry = fromJson(CompactJsonWriter.toString(input))
      geometry match {
        case Some(g) => Some(value(g))
        case _ => None
      }
    }
  }

  def toJValue(input: SoQLValue) =
    if(SoQLNull == input) JNull
    else JsonReader.fromString(toJson(input))
}
