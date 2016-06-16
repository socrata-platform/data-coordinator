package com.socrata.datacoordinator.secondary.feedback

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.util.{JsonUtil, AutomaticJsonCodecBuilder, WrapperFieldCodec}
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
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
                        columnIdMap: Map[UserColumnId, ColumnId],
                        strategyMap: Map[UserColumnId, ComputationStrategyInfo],
                        obfuscationKey: Array[Byte],
                        computationRetriesLeft: Int,
                        mutationScriptRetriesLeft: Int,
                        resync: Boolean) {

  override def equals(any: Any): Boolean = {
    any match {
      case other: CookieSchema =>
        this.dataVersion == other.dataVersion &&
          this.copyNumber == other.copyNumber &&
          this.primaryKey == other.primaryKey &&
          this.columnIdMap == other.columnIdMap &&
          this.strategyMap == other.strategyMap &&
          java.util.Arrays.equals(this.obfuscationKey, other.obfuscationKey) && // stupid arrays
          this.computationRetriesLeft == other.computationRetriesLeft &&
          this.mutationScriptRetriesLeft == other.mutationScriptRetriesLeft &&
          this.resync == other.resync
      case _ => false
    }
  }

  override def hashCode: Int = {
    var code = 17
    code = code * 41 + (if (dataVersion == null) 0 else dataVersion.hashCode)
    code = code * 41 + (if (copyNumber == null) 0 else copyNumber.hashCode)
    code = code * 41 + primaryKey.hashCode
    code = code * 41 + (if (columnIdMap == null) 0 else columnIdMap.hashCode)
    code = code * 41 + (if (strategyMap == null) 0 else strategyMap.hashCode)
    code = code * 41 + java.util.Arrays.hashCode(obfuscationKey)
    code = code * 41 + computationRetriesLeft.hashCode
    code = code * 41 + mutationScriptRetriesLeft.hashCode
    code = code * 41 + resync.hashCode
    code
  }
}
