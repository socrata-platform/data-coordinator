package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast.{JNull, JValue, JObject}
import com.rojoma.json.v3.codec.JsonDecode
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types._

object JsonRep extends JsonColumnRep[SoQLType, SoQLValue] {

  val representedType = SoQLJson

  def fromJValue(input: JValue): Option[SoQLValue] = {
    input match {
      case JObject(m) => m.get("json").map(js => SoQLJson(js))
      case JNull => Some(SoQLNull)
      case _ => None
    }
  }

  def toJValue(input: SoQLValue): JValue = {
    input match {
      case x@SoQLJson(value) => JObject(Map("json" -> value))
      case SoQLNull => JNull
      case _ => stdBadValue
    }
  }
}
