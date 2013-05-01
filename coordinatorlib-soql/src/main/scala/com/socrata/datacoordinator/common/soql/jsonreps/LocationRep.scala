package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLLocation, SoQLType}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JNumber, JObject, JValue}

object LocationRep
  extends CodecBasedJsonColumnRep[SoQLType, SoQLValue, SoQLLocation](SoQLLocation, _.asInstanceOf[SoQLLocation], identity, SoQLNull)(LocationRepCodec.codec) {
}

object LocationRepCodec {
  val codec = new JsonCodec[SoQLLocation] {
    def encode(x: SoQLLocation): JValue =
      JObject(Map(
        "lat" -> JNumber(x.latitude),
        "lon" -> JNumber(x.longitude)
      ))

    def decode(x: JValue): Option[SoQLLocation] = x match {
      case JObject(fields) =>
        fields.get("lat") match {
          case Some(JNumber(lat)) =>
            fields.get("lon") match {
              case Some(JNumber(lon)) =>
                Some(SoQLLocation(lat.toDouble, lon.toDouble))
              case _ =>
                None
            }
          case _ =>
            None
        }
      case _ =>
        None
    }
  }
}
