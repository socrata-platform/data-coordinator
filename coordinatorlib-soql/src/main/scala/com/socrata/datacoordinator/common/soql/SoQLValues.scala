package com.socrata.datacoordinator.common.soql

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.util.AutomaticJsonCodecBuilder
import com.rojoma.json.util.JsonKey

case object SoQLNullValue

case class SoQLLocationValue(@JsonKey("lat") latitude: Double, @JsonKey("lon") longitude: Double)
object SoQLLocationValue {
  implicit val jCodec = AutomaticJsonCodecBuilder[SoQLLocationValue]
}
