package com.socrata.querycoordinator

import com.socrata.soql.types.SoQLType

import com.socrata.soql.environment.{TypeName, ColumnName}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JString, JObject}
import com.rojoma.json.matcher.{PObject, Variable}

case class RollupInfo(name: String, soql: String)
object RollupInfo {
  implicit object RollupInfoCodec extends JsonCodec[RollupInfo] {
    private implicit object SoQLTypeCodec extends JsonCodec[SoQLType] {
      def encode(t: SoQLType) = JString(t.name.name)
      def decode(v: JValue) = v match {
        case JString(s) => SoQLType.typesByName.get(TypeName(s))
        case _ => None
      }
    }

    private val nameVar = Variable[String]()
    private val soqlVar = Variable[String]()
    private val PRollupInfo = PObject(
      "name" -> nameVar,
      "soql" -> soqlVar
    )

    def encode(rollupInfoObj: RollupInfo) = {
      val RollupInfo(name, soql) = rollupInfoObj
      PRollupInfo.generate(nameVar := name, soqlVar := soql)
    }

    def decode(x: JValue) = PRollupInfo.matches(x) map { results =>
      RollupInfo(nameVar(results), soqlVar(results))
    }
  }
}
