package com.socrata.datacoordinator
package truth.metadata

import com.rojoma.json.util.{JsonKey, AutomaticJsonCodecBuilder}
import com.socrata.datacoordinator.id.{RowId, DatasetId}

trait DatasetInfoLike extends Product {
  val systemId: DatasetId
  val datasetName: String
  val tableBaseBase: String
  val nextRowId: RowId

  lazy val tableBase = tableBaseBase + "_" + systemId.underlying
  lazy val logTableName = tableBase + "_log"
}

case class UnanchoredDatasetInfo(@JsonKey("sid") systemId: DatasetId,
                                 @JsonKey("name") datasetName: String,
                                 @JsonKey("tbase") tableBaseBase: String,
                                 @JsonKey("rid") nextRowId: RowId) extends DatasetInfoLike

object UnanchoredDatasetInfo extends ((DatasetId, String, String, RowId) => UnanchoredDatasetInfo) {
  override def toString = "DatasetInfo"
  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredDatasetInfo]
}

/** This class should not be instantiated except by a [[com.socrata.datacoordinator.truth.metadata.DatasetMapReader]]
  * or [[com.socrata.datacoordinator.truth.metadata.DatasetMapWriter]].
  * @param tag Guard against a non-map accidentially instantiating this.
  */
case class DatasetInfo(systemId: DatasetId, datasetName: String, tableBaseBase: String, nextRowId: RowId)(implicit tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag) extends DatasetInfoLike {
  def unanchored: UnanchoredDatasetInfo = UnanchoredDatasetInfo(systemId, datasetName, tableBaseBase, nextRowId)
}
