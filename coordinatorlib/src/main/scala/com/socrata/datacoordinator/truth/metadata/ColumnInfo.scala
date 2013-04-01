package com.socrata.datacoordinator
package truth.metadata

import com.socrata.datacoordinator.id.ColumnId
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, JsonKey}
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JString, JValue}

sealed trait AbstractColumnInfoLike extends Product {
  val systemId: ColumnId
  val logicalName: ColumnName
  val typeName: TypeName
  val isSystemPrimaryKey: Boolean
  val isUserPrimaryKey: Boolean
}

sealed trait ColumnInfoLike extends AbstractColumnInfoLike {
  val physicalColumnBaseBase: String

  lazy val physicalColumnBase = physicalColumnBaseBase + "_" + systemId.underlying
}

case class UnanchoredColumnInfo(@JsonKey("sid") systemId: ColumnId,
                                @JsonKey("name") logicalName: ColumnName,
                                @JsonKey("type") typeName: TypeName,
                                @JsonKey("base") physicalColumnBaseBase: String,
                                @JsonKey("spk") isSystemPrimaryKey: Boolean,
                                @JsonKey("upk") isUserPrimaryKey: Boolean) extends ColumnInfoLike

object UnanchoredColumnInfo extends ((ColumnId, ColumnName, TypeName, String, Boolean, Boolean) => UnanchoredColumnInfo) {
  override def toString = "UnanchoredColumnInfo"
  implicit val columnNameCodec = new JsonCodec[ColumnName] {
    def encode(x: ColumnName): JValue = JString(x.name)

    def decode(x: JValue): Option[ColumnName] = x match {
      case JString(s) => Some(ColumnName(s))
      case _ => None
    }
  }
  implicit val typeNameCodec = new JsonCodec[TypeName] {
    def encode(x: TypeName): JValue = JString(x.name)

    def decode(x: JValue): Option[TypeName] = x match {
      case JString(s) => Some(TypeName(s))
      case _ => None
    }
  }
  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredColumnInfo]
}

/** This class should not be instantiated except by a [[com.socrata.datacoordinator.truth.metadata.DatasetMapReader]]
  * or [[com.socrata.datacoordinator.truth.metadata.DatasetMapWriter]].
  * @param tag Guard against a non-map accidentially instantiating this.
  */
case class ColumnInfo(copyInfo: CopyInfo, systemId: ColumnId, logicalName: ColumnName, typeName: TypeName, physicalColumnBaseBase: String, isSystemPrimaryKey: Boolean, isUserPrimaryKey: Boolean)(implicit tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag)  extends ColumnInfoLike {
  def unanchored: UnanchoredColumnInfo = UnanchoredColumnInfo(systemId, logicalName, typeName, physicalColumnBaseBase, isSystemPrimaryKey, isUserPrimaryKey)
}
