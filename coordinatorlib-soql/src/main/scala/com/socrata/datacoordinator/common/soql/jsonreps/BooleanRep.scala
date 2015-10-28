package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.ast._

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLBoolean, SoQLType}
import com.socrata.soql.environment.ColumnName

// This can't be a codec-based rep because of some weirdness involving
// pattern matching with an implicit ClassTag in scope + a boxed primitive
object BooleanRep extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLBoolean

  def fromJValue(input: JValue): Option[SoQLValue] = input match {
    case JBoolean(b) => Some(SoQLBoolean.canonicalValue(b))
    case JNull => Some(SoQLNull)
    case _ => None
  }

  def toJValue(input: SoQLValue): JValue = input match {
    case SoQLBoolean(b) => JBoolean(b)
    case SoQLNull => JNull
    case _ => stdBadValue
  }
}
