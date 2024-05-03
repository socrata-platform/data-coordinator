package com.socrata.datacoordinator.truth

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError, FieldEncode, FieldDecode}

import com.socrata.soql.analyzer2.MetaTypes

import com.socrata.datacoordinator.id.{DatasetInternalName, DatasetResourceName}

final abstract class RollupMetaTypes extends MetaTypes {
  type DatabaseTableNameImpl = RollupMetaTypes.TableName

  type ResourceNameScope = DataCoordinatorMetaTypes#ResourceNameScope
  type ColumnType = DataCoordinatorMetaTypes#ColumnType
  type ColumnValue = DataCoordinatorMetaTypes#ColumnValue
  type DatabaseColumnNameImpl = DataCoordinatorMetaTypes#DatabaseColumnNameImpl
}

object RollupMetaTypes {
  // This is basically Either[DatasetInternalName, DatasetResourceName] but it
  // can be used as a map key.  It's temporary until we migrate existing rollups
  // to be all resource-name based
  sealed abstract class TableName
  object TableName {
    case class InternalName(internalName: DatasetInternalName) extends TableName
    case class ResourceName(resourceName: DatasetResourceName) extends TableName

    implicit val jCodec = new JsonEncode[TableName] with JsonDecode[TableName] {
      def encode(tableName: TableName) =
        tableName match {
          case InternalName(in) => JsonEncode.toJValue(in)
          case ResourceName(rn) => JsonEncode.toJValue(rn)
        }

      def decode(x: JValue) =
        JsonDecode.fromJValue[DatasetInternalName](x) match {
          case Right(in) =>
            Right(InternalName(in))
          case Left(err1) =>
            JsonDecode.fromJValue[DatasetResourceName](x) match {
              case Right(rn) => Right(ResourceName(rn))
              case Left(err2) => Left(DecodeError.join(Seq(err1, err2)))
            }
        }
    }

    implicit def fCodec = new FieldEncode[TableName] with FieldDecode[TableName] {
      def encode(tn: TableName) =
        tn match {
          case InternalName(in) => in.underlying
          case ResourceName(rn) => rn.underlying
        }

      def decode(x: String) =
        DatasetInternalName(x) match {
          case Some(in) =>
            Right(InternalName(in))
          case None =>
            Right(ResourceName(DatasetResourceName(x)))
        }
    }
  }
}
