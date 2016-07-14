package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLID, SoQLType}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.v3.ast.{JNull, JString, JValue}
import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.datacoordinator.id.RowId

class IDRep(StringRep: SoQLID.StringRep) extends JsonColumnRep[SoQLType, SoQLValue] {
  def fromJValue(input: JValue): Option[SoQLID] = input match {
    case JString(StringRep(id)) => Some(id)
    case _ => None
  }

  def toJValue(value: SoQLValue): JValue = value match {
    case id: SoQLID => JString(StringRep(id))
    case SoQLNull => JNull
    case _ => stdBadValue
  }

  val representedType: SoQLType = SoQLID
}
