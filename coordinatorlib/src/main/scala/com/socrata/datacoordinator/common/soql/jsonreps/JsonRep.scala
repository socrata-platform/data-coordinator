package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast.{JNull, JValue}
import com.rojoma.json.v3.codec.JsonDecode
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types._

object JsonRep extends JsonColumnRep[SoQLType, SoQLValue] {

  val representedType = SoQLJson

  def fromJValue(input: JValue): Option[SoQLValue] = {
    Some(SoQLJson(input))
  }

  def toJValue(input: SoQLValue): JValue = {
    input match {
      case x@SoQLJson(value) => value
      case SoQLNull => JNull // TODO: this needs to be disambiguated from SoQLNull? is it?
      case _ => stdBadValue
    }
  }
}
