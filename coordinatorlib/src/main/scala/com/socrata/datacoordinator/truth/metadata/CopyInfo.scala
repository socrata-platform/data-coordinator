package com.socrata.datacoordinator
package truth.metadata

import scala.runtime.ScalaRunTime

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{PObject, Variable}
import com.rojoma.json.ast.JValue

import com.socrata.datacoordinator.id.CopyId

trait CopyInfo extends Product {
  val datasetInfo: DatasetInfo
  val systemId: CopyId
  val copyNumber: Long
  val lifecycleStage: LifecycleStage
  val dataVersion: Long

  lazy val dataTableName = datasetInfo.tableBase + "_" + copyNumber

  override final def hashCode = ScalaRunTime._hashCode(this)
  override final def productPrefix = "CopyInfo"
  override final def toString = ScalaRunTime._toString(this)
  override final def equals(o: Any) = o match {
    case that: CopyInfo =>
      ScalaRunTime._equals(this, that)
    case _ =>
      false
  }
}

case class SimpleVersionInfo(datasetInfo: DatasetInfo, systemId: CopyId, copyNumber: Long, lifecycleStage: LifecycleStage, dataVersion: Long) extends CopyInfo

object CopyInfo {
  implicit object jCodec extends JsonCodec[CopyInfo] {
    private implicit def lifecycleCodec = LifecycleStage.jCodec

    val datasetInfoVar = Variable[DatasetInfo]
    val systemIdVar = Variable[CopyId]
    val copyNumberVar = Variable[Long]
    val lifecycleStageVar = Variable[LifecycleStage]
    val dataVersionVar = Variable[Long]

    val PVersionInfo = PObject(
      "di" -> datasetInfoVar,
      "sid" -> systemIdVar,
      "num" -> copyNumberVar,
      "stage" -> lifecycleStageVar,
      "ver" -> dataVersionVar
    )

    def encode(x: CopyInfo) = PVersionInfo.generate(
      datasetInfoVar := x.datasetInfo,
      systemIdVar := x.systemId,
      copyNumberVar := x.copyNumber,
      lifecycleStageVar := x.lifecycleStage,
      dataVersionVar := x.dataVersion
    )

    def decode(x: JValue) = PVersionInfo.matches(x) map { results =>
      SimpleVersionInfo(
        datasetInfo = datasetInfoVar(results),
        systemId = systemIdVar(results),
        copyNumber = copyNumberVar(results),
        lifecycleStage = lifecycleStageVar(results),
        dataVersion = dataVersionVar(results)
      )
    }
  }
}
