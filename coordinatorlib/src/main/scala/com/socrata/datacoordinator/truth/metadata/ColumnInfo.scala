package com.socrata.datacoordinator
package truth.metadata

import scala.runtime.ScalaRunTime

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{PObject, Variable}
import com.rojoma.json.ast.JValue

import com.socrata.datacoordinator.id.ColumnId

trait UnanchoredColumnInfo extends Product {
  val systemId: ColumnId
  val logicalName: String
  val typeName: String
  val physicalColumnBaseBase: String
  val isSystemPrimaryKey: Boolean
  val isUserPrimaryKey: Boolean

  protected def canonicalType: Class[_ <: UnanchoredColumnInfo]

  lazy val physicalColumnBase = physicalColumnBaseBase + "_" + systemId.underlying

  override final def hashCode = ScalaRunTime._hashCode(this)
  override final def productPrefix = "ColumnInfo"
  override final def toString = ScalaRunTime._toString(this)
  override final def equals(o: Any) = o match {
    case that if canonicalType.isInstance(that) =>
      ScalaRunTime._equals(this, that)
    case _ =>
      false
  }
}

case class SimpleUnanchoredColumnInfo(systemId: ColumnId, logicalName: String, typeName: String, physicalColumnBaseBase: String, isSystemPrimaryKey: Boolean, isUserPrimaryKey: Boolean) extends UnanchoredColumnInfo {
  final def canonicalType = classOf[UnanchoredColumnInfo]
}

object UnanchoredColumnInfo {
  implicit object jCodec extends JsonCodec[UnanchoredColumnInfo] {
    val systemIdVar = Variable[ColumnId]
    val logicalNameVar = Variable[String]
    val typeNameVar = Variable[String]
    val physicalColumnBaseBaseVar = Variable[String]
    val isSystemPrimaryKeyVar = Variable[Boolean]
    val isUserPrimaryKeyVar = Variable[Boolean]

    val PColumnInfo = PObject(
      "sid" -> systemIdVar,
      "name" -> logicalNameVar,
      "type" -> typeNameVar,
      "base" -> physicalColumnBaseBaseVar,
      "spk" -> isSystemPrimaryKeyVar,
      "upk" -> isUserPrimaryKeyVar
    )

    def encode(x: UnanchoredColumnInfo) = PColumnInfo.generate(
      systemIdVar := x.systemId,
      logicalNameVar := x.logicalName,
      typeNameVar := x.typeName,
      physicalColumnBaseBaseVar := x.physicalColumnBaseBase,
      isSystemPrimaryKeyVar := x.isSystemPrimaryKey,
      isUserPrimaryKeyVar := x.isUserPrimaryKey
    )

    def decode(x: JValue) = PColumnInfo.matches(x) map { results =>
      SimpleUnanchoredColumnInfo(
        systemId = systemIdVar(results),
        logicalName = logicalNameVar(results),
        typeName = typeNameVar(results),
        physicalColumnBaseBase = physicalColumnBaseBaseVar(results),
        isSystemPrimaryKey = isSystemPrimaryKeyVar(results),
        isUserPrimaryKey = isUserPrimaryKeyVar(results)
      )
    }
  }
}

trait ColumnInfo extends UnanchoredColumnInfo {
  val copyInfo: CopyInfo
  def withCopyInfo(ci: CopyInfo) = SimpleColumnInfo(ci, systemId, logicalName, typeName, physicalColumnBaseBase, isSystemPrimaryKey, isUserPrimaryKey)
}

case class SimpleColumnInfo(copyInfo: CopyInfo, systemId: ColumnId, logicalName: String, typeName: String, physicalColumnBaseBase: String, isSystemPrimaryKey: Boolean, isUserPrimaryKey: Boolean) extends ColumnInfo {
  final def canonicalType = classOf[ColumnInfo]
}

object ColumnInfo {
  implicit object jCodec extends JsonCodec[ColumnInfo] {
    val copyInfoVar = Variable[CopyInfo]
    val systemIdVar = Variable[ColumnId]
    val logicalNameVar = Variable[String]
    val typeNameVar = Variable[String]
    val physicalColumnBaseBaseVar = Variable[String]
    val isSystemPrimaryKeyVar = Variable[Boolean]
    val isUserPrimaryKeyVar = Variable[Boolean]

    val PColumnInfo = PObject(
      "copy" -> copyInfoVar,
      "sid" -> systemIdVar,
      "name" -> logicalNameVar,
      "type" -> typeNameVar,
      "base" -> physicalColumnBaseBaseVar,
      "spk" -> isSystemPrimaryKeyVar,
      "upk" -> isUserPrimaryKeyVar
    )

    def encode(x: ColumnInfo) = PColumnInfo.generate(
      copyInfoVar := x.copyInfo,
      systemIdVar := x.systemId,
      logicalNameVar := x.logicalName,
      typeNameVar := x.typeName,
      physicalColumnBaseBaseVar := x.physicalColumnBaseBase,
      isSystemPrimaryKeyVar := x.isSystemPrimaryKey,
      isUserPrimaryKeyVar := x.isUserPrimaryKey
    )

    def decode(x: JValue) = PColumnInfo.matches(x) map { results =>
      SimpleColumnInfo(
        copyInfo = copyInfoVar(results),
        systemId = systemIdVar(results),
        logicalName = logicalNameVar(results),
        typeName = typeNameVar(results),
        physicalColumnBaseBase = physicalColumnBaseBaseVar(results),
        isSystemPrimaryKey = isSystemPrimaryKeyVar(results),
        isUserPrimaryKey = isUserPrimaryKeyVar(results)
      )
    }
  }
}
