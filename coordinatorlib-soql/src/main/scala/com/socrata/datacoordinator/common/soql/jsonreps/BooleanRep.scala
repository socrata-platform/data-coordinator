package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast._

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLBoolean, SoQLType}
import com.socrata.datacoordinator.common.soql.{SoQLBooleanValue, SoQLValue, SoQLNullValue}
import com.socrata.soql.environment.ColumnName

// This can't be a codec-based rep because of some weirdness involving
// pattern matching with an implicit ClassTag in scope + a boxed primitive
class BooleanRep(val name: ColumnName) extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLBoolean

  def fromJValue(input: JValue) = input match {
    case JBoolean(b) => Some(SoQLBooleanValue.canonical(b))
    case JNull => Some(SoQLNullValue)
    case _ => None
  }

  def toJValue(input: SoQLValue) = input match {
    case SoQLBooleanValue(b) => JBoolean(b)
    case SoQLNullValue => JNull
    case _ => stdBadValue
  }
}
