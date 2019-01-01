package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast.{JNull, JValue}
import com.rojoma.json.v3.codec.JsonDecode
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types._

object DocumentRep extends JsonColumnRep[SoQLType, SoQLValue] {

  val representedType = SoQLDocument

  def fromJValue(input: JValue): Option[SoQLValue] = {
    input match {
      case JNull => Some(SoQLNull)
      case _ => JsonDecode[SoQLDocument].decode(input).right.toOption
    }
  }

  def toJValue(input: SoQLValue): JValue = {
    input match {
      case x@SoQLDocument(_, _, _) => SoQLDocument.jCodec.encode(x)
      case SoQLNull => JNull
      case _ => stdBadValue
    }
  }
}
