package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.ast._

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types._
import com.socrata.soql.environment.ColumnName

class ArrayRep(val name: ColumnName) extends JsonColumnRep[SoQLType, SoQLValue] {
  val representedType = SoQLArray

  def fromJValue(input: JValue) = input match {
    case arr: JArray => Some(SoQLArray(arr))
    case JNull => Some(SoQLNull)
    case _ => None
  }

  def toJValue(input: SoQLValue) = input match {
    case SoQLArray(arr) => arr
    case SoQLNull => JNull
    case _ => stdBadValue
  }
}
