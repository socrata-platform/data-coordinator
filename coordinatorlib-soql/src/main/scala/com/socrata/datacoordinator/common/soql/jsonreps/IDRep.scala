package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.{JsonColumnRep, CodecBasedJsonColumnRep}
import com.socrata.soql.types.{SoQLID, SoQLType}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.common.soql.{SoQLRep, SoQLNullValue}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.ast.{JNull, JString, JValue}

class IDRep(val name: ColumnName, obfuscationContext: SoQLRep.IdObfuscationContext) extends JsonColumnRep[SoQLType, Any] {
  def fromJValue(input: JValue): Option[RowId] = input match {
    case JString(obfuscated) => obfuscationContext.deobfuscate(obfuscated)
    case _ => None
  }

  def toJValue(value: Any): JValue = value match {
    case rowId: RowId => JString(obfuscationContext.obfuscate(rowId))
    case SoQLNullValue => JNull
    case _ => stdBadValue
  }

  val representedType: SoQLType = SoQLID
}
