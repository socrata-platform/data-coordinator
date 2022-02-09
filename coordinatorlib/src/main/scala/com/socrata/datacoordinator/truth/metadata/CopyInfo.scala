package com.socrata.datacoordinator
package truth.metadata

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKey}
import com.socrata.datacoordinator.id.CopyId
import org.joda.time.DateTime
import com.socrata.thirdparty.json.AdditionalJsonCodecs._


sealed trait CopyInfoLike extends Product {
  val systemId: CopyId
  val copyNumber: Long
  val lifecycleStage: LifecycleStage
  val dataVersion: Long
  val shapeDataVersion: Long
  val lastModified: DateTime
}

case class UnanchoredCopyInfo(@JsonKey("sid") systemId: CopyId,
                              @JsonKey("num") copyNumber: Long,
                              @JsonKey("stage") lifecycleStage: LifecycleStage,
                              @JsonKey("ver") dataVersion: Long,
                              @JsonKey("sver") shapeDataVersion: Long,
                              @JsonKey("lm") lastModified: DateTime) extends CopyInfoLike

object UnanchoredCopyInfo extends ((CopyId, Long, LifecycleStage, Long, Long, DateTime) => UnanchoredCopyInfo) {
  override def toString = "UnanchoredCopyInfo"

  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredCopyInfo]
}

/** This class should not be instantiated except by a [[com.socrata.datacoordinator.truth.metadata.DatasetMapReader]]
  * or [[com.socrata.datacoordinator.truth.metadata.DatasetMapWriter]].
  * @param tag Guard against a non-map accidentially instantiating this.
  */
case class CopyInfo(datasetInfo: DatasetInfo, systemId: CopyId, copyNumber: Long, lifecycleStage: LifecycleStage, dataVersion: Long, shapeDataVersion: Long, lastModified: DateTime, tableModifier: Option[Long])(implicit tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag) extends CopyInfoLike {
  lazy val dataTableName = datasetInfo.tableBase + "_" + copyNumber + tableModifier.fold("")("_" + _)
  def unanchored: UnanchoredCopyInfo = UnanchoredCopyInfo(systemId, copyNumber,lifecycleStage, dataVersion, shapeDataVersion, lastModified)
}
