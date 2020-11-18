package com.socrata.datacoordinator.id

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode, FieldEncode, FieldDecode}

import scala.util.Try

case class DatasetInternalName(instance: String, datasetId: DatasetId) {
  val underlying = s"$instance.${datasetId.underlying}"
  override def toString = s"DatasetInternalName($underlying)"
}

object DatasetInternalName {
  def apply(internalName: String): Option[DatasetInternalName] = {
    val parts = internalName.split('.')
    Try(DatasetInternalName(parts(0), new DatasetId(parts(1).toLong))).toOption
  }

  implicit val jCodec = new JsonDecode[DatasetInternalName] with JsonEncode[DatasetInternalName] {
    def encode(datasetInternalName: DatasetInternalName) = JString(datasetInternalName.underlying)
    def decode(v: JValue): Either[DecodeError, DatasetInternalName] = v match {
      case s: JString =>
        DatasetInternalName(s.string) match {
          case Some(din) => Right(din)
          case None => Left(DecodeError.InvalidValue(s))
        }
      case other => Left(DecodeError.InvalidType(JString, other.jsonType))
    }
  }

  implicit val fCodec = new FieldEncode[DatasetInternalName] with FieldDecode[DatasetInternalName] {
    def encode(datasetInternalName: DatasetInternalName) = datasetInternalName.underlying
    def decode(s: String): Either[DecodeError.InvalidField, DatasetInternalName] =
      DatasetInternalName(s) match {
        case Some(din) => Right(din)
        case None => Left(DecodeError.InvalidField(s))
      }
  }
}
