package com.socrata.datacoordinator.secondary.feedback.instance

import com.rojoma.json.v3.ast.JValue
import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.types._

object SoQLValueRep extends (Array[Byte] => SoQLType => SoQLValue => JValue) {

  def apply(obfuscationKey: Array[Byte]): (SoQLType => SoQLValue => JValue) = {
    val cryptCipher = new CryptProvider(obfuscationKey)
    val rep = SoQLRep.jsonRep(new SoQLID.StringRep(cryptCipher), new SoQLVersion.StringRep(cryptCipher))

    { typ: SoQLType =>
      { value: SoQLValue =>
        rep(typ).toJValue(value)
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
