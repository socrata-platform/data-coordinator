package com.socrata.datacoordinator
package truth.metadata

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKey}
import com.socrata.datacoordinator.id.CopyId
import org.joda.time.DateTime
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import org.joda.time.format.ISODateTimeFormat
import com.rojoma.json.v3.ast.{JString, JValue}

sealed trait CopyInfoLike extends Product {
  val systemId: CopyId
  val copyNumber: Long
  val lifecycleStage: LifecycleStage
  val dataVersion: Long
  val lastModified: DateTime
}

case class UnanchoredCopyInfo(@JsonKey("sid") systemId: CopyId,
                              @JsonKey("num") copyNumber: Long,
                              @JsonKey("stage") lifecycleStage: LifecycleStage,
                              @JsonKey("ver") dataVersion: Long,
                              @JsonKey("lm") lastModified: DateTime) extends CopyInfoLike

object UnanchoredCopyInfo extends ((CopyId, Long, LifecycleStage, Long, DateTime) => UnanchoredCopyInfo) {
  override def toString = "UnanchoredCopyInfo"

  private implicit object DateTimeCodec extends JsonDecode[DateTime] with JsonEncode[DateTime] {
    val formatter = ISODateTimeFormat.dateTime
    val parser = ISODateTimeFormat.dateTimeParser
    def encode(x: DateTime): JValue = JString(formatter.print(x))
    def decode(x: JValue): JsonDecode.DecodeResult[DateTime] = x match {
      case JString(s) =>
        try {
          Right(parser.parseDateTime(x.toString()))
        } catch {
          case _: IllegalArgumentException =>
            Left(DecodeError.InvalidValue(x))
        }
      case other =>
        Left(DecodeError.InvalidType(JString, other.jsonType))
    }
  }
  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredCopyInfo]
}

/** This class should not be instantiated except by a [[com.socrata.datacoordinator.truth.metadata.DatasetMapReader]]
  * or [[com.socrata.datacoordinator.truth.metadata.DatasetMapWriter]].
  * @param tag Guard against a non-map accidentially instantiating this.
  */
case class CopyInfo(datasetInfo: DatasetInfo, systemId: CopyId, copyNumber: Long, lifecycleStage: LifecycleStage, dataVersion: Long, lastModified: DateTime, tableModifier: Option[Long])(implicit tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag) extends CopyInfoLike {
  lazy val dataTableName = datasetInfo.tableBase + "_" + copyNumber + tableModifier.fold("")("_" + _)
  def unanchored: UnanchoredCopyInfo = UnanchoredCopyInfo(systemId, copyNumber,lifecycleStage, dataVersion, lastModified)
}
