package com.socrata.datacoordinator.secondary.feedback.instance

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.soql.environment.TypeName
import com.socrata.soql.functions.SoQLTypeInfo
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.types._

object SoQLValueRepFor extends (Array[Byte] => SoQLType => SoQLValue => JValue) {

  def apply(obfuscationKey: Array[Byte]): (SoQLType => SoQLValue => JValue) = {
    val cryptCipher = new CryptProvider(obfuscationKey)
    val rep = SoQLRep.jsonRep(cryptCipher, true)

    { typ: SoQLType =>
      { value: SoQLValue =>
        rep(typ).toJValue(value)
      }
    }
  }

}

object SoQLValueRepFrom extends (Array[Byte] => SoQLType => JValue => Option[SoQLValue]) {

  def apply(obfuscationKey: Array[Byte]): (SoQLType => JValue => Option[SoQLValue]) = {
    val cryptCipher = new CryptProvider(obfuscationKey)
    val rep = SoQLRep.jsonRep(cryptCipher, true)

    { typ: SoQLType =>
      { value: JValue =>
        rep(typ).fromJValue(value).toOption
      }
    }
  }

}

object SoQLTypeFor extends (SoQLValue => Option[SoQLType]) {

  def apply(value: SoQLValue) = value match {
    case SoQLNull => None
    case other => Some(other.typ)
  }

}

object SoQLTypeFromJValue extends (JValue => Option[SoQLType]) {

  def apply(value: JValue) = value match {
    case JString(name) => SoQLTypeInfo.typeFor(TypeName(name))
    case _ => None
  }

}

object SoQLEstimateSize extends (SoQLValue => Int) {
  def apply(value: SoQLValue) = value.typ match {
    case SoQLID => 8
    case SoQLVersion => 8
    case SoQLNull => 8
    case other => CompactJsonWriter.toString(SoQLRep.jsonRepsMinusIdAndVersion(other).toJValue(value)).length // ick
  }
}
