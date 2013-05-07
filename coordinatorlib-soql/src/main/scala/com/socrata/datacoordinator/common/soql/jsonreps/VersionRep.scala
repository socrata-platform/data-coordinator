package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLVersion, SoQLType}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.ast.{JNull, JString, JValue}
import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.datacoordinator.id.RowVersion

class VersionRep(obfuscationContext: SoQLRep.VersionObfuscationContext) extends JsonColumnRep[SoQLType, SoQLValue] {
  def fromJValue(input: JValue) = input match {
    case JString(obfuscated) =>
      obfuscationContext.deobfuscate(obfuscated).map { r => SoQLVersion(r.underlying) }
    case JNull =>
      Some(SoQLNull)
    case _ =>
      None
  }

  def toJValue(value: SoQLValue): JValue = value match {
    case SoQLVersion(rowId) => JString(obfuscationContext.obfuscate(new RowVersion(rowId)))
    case SoQLNull => JNull
    case _ => stdBadValue
  }

  val representedType: SoQLType = SoQLVersion
}
