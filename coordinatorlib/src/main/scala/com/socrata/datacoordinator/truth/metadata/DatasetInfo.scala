package com.socrata.datacoordinator
package truth.metadata

import com.rojoma.json.util.{JsonKey, AutomaticJsonCodecBuilder}
import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JString, JValue}

trait DatasetInfoLike extends Product {
  val systemId: DatasetId
  val datasetName: String
  val tableBaseBase: String
  val nextCounterValue: Long
  val localeName: String
  val obfuscationKey: Array[Byte]

  lazy val tableBase = tableBaseBase + "_" + systemId.underlying
  lazy val logTableName = tableBase + "_log"
}

case class UnanchoredDatasetInfo(@JsonKey("sid") systemId: DatasetId,
                                 @JsonKey("name") datasetName: String,
                                 @JsonKey("tbase") tableBaseBase: String,
                                 @JsonKey("ctr") nextCounterValue: Long,
                                 @JsonKey("locale") localeName: String,
                                 @JsonKey("obfkey") obfuscationKey: Array[Byte]) extends DatasetInfoLike

object UnanchoredDatasetInfo extends ((DatasetId, String, String, Long, String, Array[Byte]) => UnanchoredDatasetInfo) {
  override def toString = "DatasetInfo"
  private implicit val byteCodec = new JsonCodec[Array[Byte]] {
    def encode(x: Array[Byte]): JValue =
      JString(new sun.misc.BASE64Encoder().encode(x))

    def decode(x: JValue): Option[Array[Byte]] = x match {
      case JString(s) =>
        try { Some(new sun.misc.BASE64Decoder().decodeBuffer(s)) }
        catch { case _: java.io.IOException => None }
      case _ =>
        None
    }
  }
  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredDatasetInfo]
}

/** This class should not be instantiated except by a [[com.socrata.datacoordinator.truth.metadata.DatasetMapReader]]
  * or [[com.socrata.datacoordinator.truth.metadata.DatasetMapWriter]].
  * @param tag Guard against a non-map accidentially instantiating this.
  */
case class DatasetInfo(systemId: DatasetId, datasetName: String, tableBaseBase: String, nextCounterValue: Long, localeName: String, obfuscationKey: Array[Byte])(implicit tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag) extends DatasetInfoLike {
  def unanchored: UnanchoredDatasetInfo = UnanchoredDatasetInfo(systemId, datasetName, tableBaseBase, nextCounterValue, localeName, obfuscationKey)

  override def equals(o: Any) = o match {
    case that: DatasetInfo =>
      this.systemId == that.systemId &&
        this.datasetName == that.datasetName &&
        this.tableBaseBase == that.tableBaseBase &&
        this.nextCounterValue == that.nextCounterValue &&
        this.localeName == that.localeName &&
        java.util.Arrays.equals(this.obfuscationKey, that.obfuscationKey) // thanks, java
    case _ =>
      false
  }
}
