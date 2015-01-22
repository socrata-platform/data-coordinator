package com.socrata.querycoordinator

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.querycoordinator.util.SoQLTypeCodec

case class RollupInfo(name: String, soql: String)
object RollupInfo {
  implicit val jCodec = AutomaticJsonCodecBuilder[RollupInfo]
  private implicit val soQLTypeCodec = SoQLTypeCodec
}
