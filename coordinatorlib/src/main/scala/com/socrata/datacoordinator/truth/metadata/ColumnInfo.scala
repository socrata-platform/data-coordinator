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
  val isSystemPrimaryKey: Boolean
  val isUserPrimaryKey: Boolean
  val typeName: String
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
                                @JsonKey("name") logicalName: ColumnName,
                                @JsonKey("type") typeName: String,
                                @JsonKey("base") physicalColumnBaseBase: String,
                                @JsonKey("spk") isSystemPrimaryKey: Boolean,
                                @JsonKey("upk") isUserPrimaryKey: Boolean) extends ColumnInfoLike

object UnanchoredColumnInfo extends ((ColumnId, ColumnName, String, String, Boolean, Boolean) => UnanchoredColumnInfo) {
  override def toString = "UnanchoredColumnInfo"
  implicit val columnNameCodec = new JsonCodec[ColumnName] {
    def encode(x: ColumnName): JValue = JString(x.name)

    def decode(x: JValue): Option[ColumnName] = x match {
      case JString(s) => Some(ColumnName(s))
      case _ => None
    }
  }
  implicit val jCodec = AutomaticJsonCodecBuilder[UnanchoredColumnInfo]
}

/** This class should not be instantiated except by a [[com.socrata.datacoordinator.truth.metadata.DatasetMapReader]]
  * or [[com.socrata.datacoordinator.truth.metadata.DatasetMapWriter]].
  * @param tag Guard against a non-map accidentially instantiating this.
  */
case class ColumnInfo[CT](copyInfo: CopyInfo, systemId: ColumnId, logicalName: ColumnName, typ: CT, physicalColumnBaseBase: String, isSystemPrimaryKey: Boolean, isUserPrimaryKey: Boolean)(implicit val typeNamespace: TypeNamespace[CT], tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag)  extends ColumnInfoLike {
  lazy val typeName = typeNamespace.nameForType(typ)
  def unanchored: UnanchoredColumnInfo = UnanchoredColumnInfo(systemId, logicalName, typeName, physicalColumnBaseBase, isSystemPrimaryKey, isUserPrimaryKey)
}

trait TypeNamespace[CT] {
  def nameForType(typ: CT): String
  def typeForName(datasetInfo: DatasetInfo, typeName: String): CT
  def typeForUserType(typeName: TypeName): Option[CT]
  def userTypeForType(typ: CT): TypeName
}
