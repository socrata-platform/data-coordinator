package com.socrata.datacoordinator.truth.metadata

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKey}
import com.socrata.datacoordinator.id.{IndexId, IndexName}

sealed trait IndexInfoLike extends Product {
  val systemId: IndexId
  val name: IndexName
  val expressions: String
  val filter: Option[String]
}

case class UnanchoredIndexInfo(@JsonKey("sid") systemId: IndexId,
                               @JsonKey("name") name: IndexName,
                               @JsonKey("expressions") expressions: String,
                               @JsonKey("filter") filter: Option[String]) extends IndexInfoLike

object UnanchoredIndexInfo extends ((IndexId, IndexName, String, Option[String]) => UnanchoredIndexInfo) {
  override def toString = "UnanchoredIndexInfo"

  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredIndexInfo]
}

case class IndexInfo(systemId: IndexId, copyInfo: CopyInfo, name: IndexName, expressions: String, filter: Option[String]) extends IndexInfoLike {
  def unanchored: UnanchoredIndexInfo = UnanchoredIndexInfo(systemId, name, expressions, filter)
}