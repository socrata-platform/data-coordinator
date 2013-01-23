package com.socrata.datacoordinator
package truth.metadata

import com.socrata.datacoordinator.id.ColumnId
import com.rojoma.json.util.AutomaticJsonCodecBuilder
import scala.runtime.ScalaRunTime
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{PObject, Variable}
import com.rojoma.json.ast.JValue

trait ColumnInfo extends Product {
  val versionInfo: VersionInfo
  val systemId: ColumnId
  val logicalName: String
  val typeName: String
  val physicalColumnBaseBase: String
  val isUserPrimaryKey: Boolean

  lazy val physicalColumnBase = physicalColumnBaseBase + "_" + systemId.underlying

  override final def hashCode = ScalaRunTime._hashCode(this)
  override final def productPrefix = "ColumnInfo"
  override final def toString = ScalaRunTime._toString(this)
  override final def equals(o: Any) = o match {
    case that: ColumnInfo =>
      ScalaRunTime._equals(this, that)
    case _ =>
      false
  }
}

case class SimpleColumnInfo(versionInfo: VersionInfo, systemId: ColumnId, logicalName: String, typeName: String, physicalColumnBaseBase: String, isUserPrimaryKey: Boolean) extends ColumnInfo

object ColumnInfo {
  implicit object jCodec extends JsonCodec[ColumnInfo] {
    val versionInfoVar = Variable[VersionInfo]
    val systemIdVar = Variable[ColumnId]
    val logicalNameVar = Variable[String]
    val typeNameVar = Variable[String]
    val physicalColumnBaseBaseVar = Variable[String]
    val isUserPrimaryKeyVar = Variable[Boolean]

    val PColumnInfo = PObject(
      "vi" -> versionInfoVar,
      "sid" -> systemIdVar,
      "name" -> logicalNameVar,
      "type" -> typeNameVar,
      "base" -> physicalColumnBaseBaseVar,
      "pk" -> isUserPrimaryKeyVar
    )

    def encode(x: ColumnInfo) = PColumnInfo.generate(
      versionInfoVar := x.versionInfo,
      systemIdVar := x.systemId,
      logicalNameVar := x.logicalName,
      typeNameVar := x.typeName,
      physicalColumnBaseBaseVar := x.physicalColumnBaseBase,
      isUserPrimaryKeyVar := x.isUserPrimaryKey
    )

    def decode(x: JValue) = PColumnInfo.matches(x) map { results =>
      SimpleColumnInfo(
        versionInfo = versionInfoVar(results),
        systemId = systemIdVar(results),
        logicalName = logicalNameVar(results),
        typeName = typeNameVar(results),
        physicalColumnBaseBase = physicalColumnBaseBaseVar(results),
        isUserPrimaryKey = isUserPrimaryKeyVar(results)
      )
    }
  }
}
