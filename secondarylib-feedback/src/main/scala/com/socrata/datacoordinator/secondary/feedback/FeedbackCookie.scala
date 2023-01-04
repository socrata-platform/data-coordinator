package com.socrata.datacoordinator.secondary.feedback

import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReaderException}
import com.rojoma.json.v3.util.{JsonUtil, AutomaticJsonCodecBuilder, WrapperFieldCodec}
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.secondary.ComputationStrategyInfo
import com.socrata.datacoordinator.secondary.Secondary.Cookie

case class CopyNumber(underlying: Long)
case class DataVersion(underlying: Long)

case class FeedbackCookie(current: CookieSchema, previous: Option[CookieSchema], errorMessage: Option[String] = None) {

  def copyCurrent(current: CookieSchema = this.current,
                  resync: Boolean = this.current.resync,
                  errorMessage: Option[String] = this.errorMessage): FeedbackCookie = {
    this.copy(
      current = current.copy(
        resync = resync),
      errorMessage = errorMessage
    )
  }
}

object FeedbackCookie {
  implicit val copyNumberCodec = AutomaticJsonCodecBuilder[CopyNumber]
  implicit val dataVersionCodec = AutomaticJsonCodecBuilder[DataVersion]
  implicit val userColumnIdCodec = WrapperFieldCodec[UserColumnId](new UserColumnId(_), _.underlying)
  implicit val strategyCodec = AutomaticJsonCodecBuilder[ComputationStrategyInfo]
  implicit val cookieSchemaCodec = AutomaticJsonCodecBuilder[CookieSchema]
  implicit val feedbackCookieCodec = AutomaticJsonCodecBuilder[FeedbackCookie]

  def encode(fbc: FeedbackCookie): Cookie = {
    Some(CompactJsonWriter.toString(feedbackCookieCodec.encode(fbc)))
  }

  def encodeOnError(reason: String, fbc: Option[FeedbackCookie]): Cookie =
    fbc.map(encode).getOrElse(Some(s"{errorMessage:$reason}"))

  def decode(ck: Cookie): Option[FeedbackCookie] = try {
    ck.flatMap(JsonUtil.parseJson[FeedbackCookie](_).right.toOption) // safely handle cookie corruption
  } catch {
    case _ : JsonReaderException => None // safely handle cookie corruption
  }
}


case class CookieSchema(dataVersion: DataVersion,
                        copyNumber: CopyNumber,
                        systemId: UserColumnId,
                        columnIdMap: Map[UserColumnId, ColumnId],
                        strategyMap: Map[UserColumnId, ComputationStrategyInfo],
                        obfuscationKey: Array[Byte],
                        resync: Boolean) {

  override def equals(any: Any): Boolean = {
    any match {
      case other: CookieSchema =>
        this.dataVersion == other.dataVersion &&
          this.copyNumber == other.copyNumber &&
          this.systemId == other.systemId &&
          this.columnIdMap == other.columnIdMap &&
          this.strategyMap == other.strategyMap &&
          java.util.Arrays.equals(this.obfuscationKey, other.obfuscationKey) && // stupid arrays
          this.resync == other.resync
      case _ => false
    }
  }

  override def hashCode: Int = {
    var code = 17
    code = code * 41 + (if (dataVersion == null) 0 else dataVersion.hashCode)
    code = code * 41 + (if (copyNumber == null) 0 else copyNumber.hashCode)
    code = code * 41 + systemId.hashCode
    code = code * 41 + (if (columnIdMap == null) 0 else columnIdMap.hashCode)
    code = code * 41 + (if (strategyMap == null) 0 else strategyMap.hashCode)
    code = code * 41 + java.util.Arrays.hashCode(obfuscationKey)
    code = code * 41 + resync.hashCode
    code
  }
}
