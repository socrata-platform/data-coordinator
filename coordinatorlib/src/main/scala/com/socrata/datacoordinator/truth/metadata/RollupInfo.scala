package com.socrata.datacoordinator
package truth.metadata

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKey}
import com.socrata.datacoordinator.id.RollupName

sealed trait RollupInfoLike extends Product {
  val name: RollupName
  val soql: String
}

case class UnanchoredRollupInfo(@JsonKey("name") name: RollupName,
                              @JsonKey("soql") soql: String) extends RollupInfoLike

object UnanchoredRollupInfo extends ((RollupName, String) => UnanchoredRollupInfo) {
  override def toString = "UnanchoredRollupInfo"

  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredRollupInfo]
}

/** This class should not be instantiated except by a [[com.socrata.datacoordinator.truth.metadata.DatasetMapReader]]
  * or [[com.socrata.datacoordinator.truth.metadata.DatasetMapWriter]].
  * @param tag Guard against a non-map accidentially instantiating this.
  */
case class RollupInfo(copyInfo: CopyInfo, name: RollupName, soql: String, rawSoql: Option[String],systemId: RollupId)(implicit tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag) extends RollupInfoLike {
  def unanchored: UnanchoredRollupInfo = UnanchoredRollupInfo(name, soql)
}
