package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast._

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLBoolean, SoQLType}
import com.socrata.datacoordinator.common.soql.SoQLNullValue

// This can't be a codec-based rep because of some weirdness involving
// pattern matching with an implicit ClassTag in scope + a boxed primitive
class BooleanRep(val name: String) extends JsonColumnRep[SoQLType, Any] {
  val representedType = SoQLBoolean

  def fromJValue(input: JValue) = input match {
    case JBoolean(b) => Some(b)
    case JNull => Some(SoQLNullValue)
    case _ => None
  }

  def toJValue(input: Any) = input match {
    case b: Boolean => JBoolean(b)
    case SoQLNullValue => JNull
    case _ => stdBadValue
  }
}
