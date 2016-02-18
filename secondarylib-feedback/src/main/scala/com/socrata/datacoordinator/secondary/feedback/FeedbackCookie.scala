package com.socrata.datacoordinator.secondary.feedback

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.util.{JsonUtil, AutomaticJsonCodecBuilder, WrapperFieldCodec}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.secondary.ComputationStrategyInfo
import com.socrata.datacoordinator.secondary.Secondary.Cookie

case class CopyNumber(underlying: Long)
case class DataVersion(underlying: Long)

case class FeedbackCookie(current: CookieSchema, previous: Option[CookieSchema])

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

  def decode(ck: Cookie): Option[FeedbackCookie] = ck match {
      case Some(str) => JsonUtil.parseJson[FeedbackCookie](str) match {
        case Right(result) => Some(result)
        case Left(error) => None // safely handle cookie corruption
      }
      case None => None
  }
}


case class CookieSchema(dataVersion: DataVersion,
                        copyNumber: CopyNumber,
                        primaryKey: UserColumnId,
                        columnIdMap: Map[UserColumnId, Long],
                        strategyMap: Map[UserColumnId, ComputationStrategyInfo],
                        obfuscationKey: Array[Byte],
                        computationRetriesLeft: Int,
                        mutationScriptRetriesLeft: Int,
                        resync: Boolean,
                        extra: JValue) {

  override def equals(any: Any): Boolean = {
    if (any == null) return false
    any match {
      case other: CookieSchema =>
        this.dataVersion == other.dataVersion &&
          this.copyNumber == other.copyNumber &&
          this.primaryKey == other.primaryKey &&
          this.columnIdMap == other.columnIdMap &&
          this.strategyMap == other.strategyMap &&
          obfuscationKeyEquals(other.obfuscationKey) && // stupid arrays
          this.computationRetriesLeft == other.computationRetriesLeft &&
          this.mutationScriptRetriesLeft == other.mutationScriptRetriesLeft &&
          this.resync == other.resync &&
          this.extra == other.extra
      case _ => false
    }
  }

  private def obfuscationKeyEquals(other: Array[Byte]): Boolean = {
    if (this.obfuscationKey == null) return other == null
    if (other == null) return false
    this.obfuscationKey.toSeq == other.toSeq
  }
}
