package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.{JsonColumnRep, CodecBasedJsonColumnRep}
import com.socrata.soql.types.{SoQLID, SoQLType}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.common.soql.{SoQLIDValue, SoQLValue, SoQLRep, SoQLNullValue}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.ast.{JNull, JString, JValue}

class IDRep(val name: ColumnName, obfuscationContext: SoQLRep.IdObfuscationContext) extends JsonColumnRep[SoQLType, SoQLValue] {
  def fromJValue(input: JValue): Option[SoQLIDValue] = input match {
    case JString(obfuscated) => obfuscationContext.deobfuscate(obfuscated).map(SoQLIDValue)
    case _ => None
  }

  def toJValue(value: SoQLValue): JValue = value match {
    case SoQLIDValue(rowId) => JString(obfuscationContext.obfuscate(rowId))
    case SoQLNullValue => JNull
    case _ => stdBadValue
  }

  val representedType: SoQLType = SoQLID
}
