package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast._

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types._
import com.socrata.soql.environment.ColumnName

object ObjectRep extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLObject

  def fromJValue(input: JValue) = input match {
    case obj: JObject => Some(SoQLObject(obj))
    case JNull => Some(SoQLNull)
    case _ => None
  }

  def toJValue(input: SoQLValue) = input match {
    case SoQLObject(obj) => obj
    case SoQLNull => JNull
    case _ => stdBadValue
  }
}
