package com.socrata.datacoordinator
package truth.metadata

import com.rojoma.json.v3.util.{JsonKey, AutomaticJsonCodecBuilder}
import com.socrata.datacoordinator.id.DatasetId
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.ast.{JString, JValue}
import com.socrata.soql.types.obfuscation.CryptProvider

trait DatasetInfoLike extends Product {
  val systemId: DatasetId
  val nextCounterValue: Long
  val localeName: String
  val obfuscationKey: Array[Byte]
  val resourceName: Option[String]

  lazy val tableBase = "t" + systemId.underlying
  lazy val auditTableName = tableBase + "_audit"
  lazy val logTableName = tableBase + "_log"
  lazy val cryptProvider = new CryptProvider(obfuscationKey)
}

case class UnanchoredDatasetInfo(@JsonKey("sid") systemId: DatasetId,
                                 @JsonKey("ctr") nextCounterValue: Long,
                                 @JsonKey("locale") localeName: String,
                                 @JsonKey("obfkey") obfuscationKey: Array[Byte],
                                 @JsonKey("resource") resourceName: Option[String]) extends DatasetInfoLike

object UnanchoredDatasetInfo extends ((DatasetId, Long, String, Array[Byte], Option[String]) => UnanchoredDatasetInfo) {
  override def toString = "DatasetInfo"
  private implicit val byteCodec = new JsonDecode[Array[Byte]] with JsonEncode[Array[Byte]] {
    def encode(x: Array[Byte]): JValue =
      JString(new sun.misc.BASE64Encoder().encode(x))

    def decode(x: JValue): JsonDecode.DecodeResult[Array[Byte]] = x match {
      case JString(s) =>
        try { Right(new sun.misc.BASE64Decoder().decodeBuffer(s)) }
        catch { case _: java.io.IOException => Left(DecodeError.InvalidValue(x)) }
      case other =>
        Left(DecodeError.InvalidType(JString, other.jsonType))
    }
  }
  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredDatasetInfo]
}

/** This class should not be instantiated except by a [[com.socrata.datacoordinator.truth.metadata.DatasetMapReader]]
  * or [[com.socrata.datacoordinator.truth.metadata.DatasetMapWriter]].
  * @param tag Guard against a non-map accidentially instantiating this.
  */
case class DatasetInfo(systemId: DatasetId, nextCounterValue: Long, val latestDataVersion: Long, localeName: String, obfuscationKey: Array[Byte], resourceName: Option[String])(implicit tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag) extends DatasetInfoLike {
  def unanchored: UnanchoredDatasetInfo = UnanchoredDatasetInfo(systemId, nextCounterValue, localeName, obfuscationKey, resourceName)

  override def equals(o: Any) = o match {
    case that: DatasetInfo =>
      this.systemId == that.systemId &&
        this.nextCounterValue == that.nextCounterValue &&
        this.latestDataVersion == that.latestDataVersion &&
        this.localeName == that.localeName &&
        java.util.Arrays.equals(this.obfuscationKey, that.obfuscationKey) && // thanks, java
        this.resourceName == that.resourceName
    case _ =>
      false
  }
}
