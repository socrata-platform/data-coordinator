package com.socrata.datacoordinator
package truth.metadata

import com.rojoma.json.v3.ast.JObject
import com.socrata.datacoordinator.id.{StrategyType, UserColumnId, ColumnId}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKey}
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.datacoordinator.util.jsoncodecs._

case class ComputationStrategyInfo(strategyType: StrategyType, sourceColumnIds: Seq[UserColumnId], parameters: JObject)

object ComputationStrategyInfo {
  implicit val JCodec = AutomaticJsonCodecBuilder[ComputationStrategyInfo]
}

sealed trait AbstractColumnInfoLike extends Product {
  val systemId: ColumnId
  val userColumnId: UserColumnId
  val fieldName: Option[ColumnName]
  val isSystemPrimaryKey: Boolean
  val isUserPrimaryKey: Boolean
  val isVersion: Boolean
  val typeName: String
  val computationStrategyInfo: Option[ComputationStrategyInfo]
}

sealed trait ColumnInfoLike extends AbstractColumnInfoLike {
  val physicalColumnBaseBase: String

  lazy val physicalColumnBase =
    ColumnInfoLike.physicalColumnBase(physicalColumnBaseBase, systemId)
}

object ColumnInfoLike {
  def physicalColumnBase(physicalColumnBaseBase: String, systemId: ColumnId) =
    physicalColumnBaseBase + "_" + systemId.underlying
}

case class UnanchoredColumnInfo(@JsonKey("sid") systemId: ColumnId,
                                @JsonKey("cid") userColumnId: UserColumnId,
                                @JsonKey("fld") fieldName: Option[ColumnName],
                                @JsonKey("type") typeName: String,
                                @JsonKey("base") physicalColumnBaseBase: String,
                                @JsonKey("spk") isSystemPrimaryKey: Boolean,
                                @JsonKey("upk") isUserPrimaryKey: Boolean,
                                @JsonKey("ver") isVersion: Boolean,
                                @JsonKey("csinfo") computationStrategyInfo: Option[ComputationStrategyInfo]) extends ColumnInfoLike

object UnanchoredColumnInfo extends ((ColumnId, UserColumnId, Option[ColumnName], String, String, Boolean, Boolean, Boolean, Option[ComputationStrategyInfo]) => UnanchoredColumnInfo) {
  override def toString = "UnanchoredColumnInfo"
  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredColumnInfo]
}

/** This class should not be instantiated except by a [[com.socrata.datacoordinator.truth.metadata.DatasetMapReader]]
  * or [[com.socrata.datacoordinator.truth.metadata.DatasetMapWriter]].
  * @param tag Guard against a non-map accidentially instantiating this.
  */
case class ColumnInfo[CT](copyInfo: CopyInfo,
                          systemId: ColumnId,
                          userColumnId: UserColumnId,
                          fieldName: Option[ColumnName],
                          typ: CT,
                          physicalColumnBaseBase: String,
                          isSystemPrimaryKey: Boolean,
                          isUserPrimaryKey: Boolean,
                          isVersion: Boolean,
                          computationStrategyInfo: Option[ComputationStrategyInfo],
                          presimplifiedZoomLevels: Seq[Int])
                         (implicit val typeNamespace: TypeNamespace[CT], tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag) extends ColumnInfoLike {
  lazy val typeName = typeNamespace.nameForType(typ)
  def unanchored: UnanchoredColumnInfo = UnanchoredColumnInfo(systemId, userColumnId, fieldName, typeName,
    physicalColumnBaseBase, isSystemPrimaryKey, isUserPrimaryKey, isVersion, computationStrategyInfo)
}

trait TypeNamespace[CT] {
  def nameForType(typ: CT): String
  def typeForName(datasetInfo: DatasetInfo, typeName: String): CT
  def typeForUserType(typeName: TypeName): Option[CT]
  def userTypeForType(typ: CT): TypeName
}
