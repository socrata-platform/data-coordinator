package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast.{JNull, JValue}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types._

object UrlRep extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLUrl

  def fromJValue(input: JValue): Option[SoQLValue] = {
    input match {
      case JNull => Some(SoQLNull)
      case _ => JsonDecode[SoQLUrl].decode(input).right.toOption
    }
  }

  def toJValue(input: SoQLValue): JValue = {
    input match {
      case x: SoQLUrl => JsonEncode.toJValue(x)
      case SoQLNull => JNull
      case _ => stdBadValue
    }
  }
}
