package com.socrata.datacoordinator
package truth.metadata

import scala.runtime.ScalaRunTime

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{PObject, Variable}
import com.rojoma.json.ast.JValue

import com.socrata.datacoordinator.id.CopyId

trait UnanchoredCopyInfo extends Product {
  val systemId: CopyId
  val copyNumber: Long
  val lifecycleStage: LifecycleStage
  val dataVersion: Long

  protected def canonicalType: Class[_ <: UnanchoredCopyInfo]

  override final def hashCode = ScalaRunTime._hashCode(this)
  override final def productPrefix = "CopyInfo"
  override final def toString = ScalaRunTime._toString(this)
  override final def equals(o: Any) = o match {
    case that if canonicalType.isInstance(that) =>
      ScalaRunTime._equals(this, that)
    case _ =>
      false
  }
}

case class SimpleUnanchoredCopyInfo(systemId: CopyId, copyNumber: Long, lifecycleStage: LifecycleStage, dataVersion: Long) extends UnanchoredCopyInfo {
  final def canonicalType = classOf[UnanchoredCopyInfo]
}

object UnanchoredCopyInfo {
  implicit object jCodec extends JsonCodec[UnanchoredCopyInfo] {
    private implicit def lifecycleCodec = LifecycleStage.jCodec

    val systemIdVar = Variable[CopyId]
    val copyNumberVar = Variable[Long]
    val lifecycleStageVar = Variable[LifecycleStage]
    val dataVersionVar = Variable[Long]

    val PCopyInfo = PObject(
      "sid" -> systemIdVar,
      "num" -> copyNumberVar,
      "stage" -> lifecycleStageVar,
      "ver" -> dataVersionVar
    )

    def encode(x: UnanchoredCopyInfo) = PCopyInfo.generate(
      systemIdVar := x.systemId,
      copyNumberVar := x.copyNumber,
      lifecycleStageVar := x.lifecycleStage,
      dataVersionVar := x.dataVersion
    )

    def decode(x: JValue) = PCopyInfo.matches(x) map { results =>
      SimpleUnanchoredCopyInfo(
        systemId = systemIdVar(results),
        copyNumber = copyNumberVar(results),
        lifecycleStage = lifecycleStageVar(results),
        dataVersion = dataVersionVar(results)
      )
    }
  }
}

trait CopyInfo extends UnanchoredCopyInfo {
  val datasetInfo: DatasetInfo

  lazy val dataTableName = datasetInfo.tableBase + "_" + copyNumber

  def withDatasetInfo(di: DatasetInfo) =
    SimpleCopyInfo(di, systemId, copyNumber, lifecycleStage, dataVersion)
}

case class SimpleCopyInfo(datasetInfo: DatasetInfo, systemId: CopyId, copyNumber: Long, lifecycleStage: LifecycleStage, dataVersion: Long) extends CopyInfo {
  final def canonicalType = classOf[CopyInfo]
}

object CopyInfo {
  implicit object jCodec extends JsonCodec[CopyInfo] {
    private implicit def lifecycleCodec = LifecycleStage.jCodec

    val datasetInfoVar = Variable[DatasetInfo]
    val systemIdVar = Variable[CopyId]
    val copyNumberVar = Variable[Long]
    val lifecycleStageVar = Variable[LifecycleStage]
    val dataVersionVar = Variable[Long]

    val PCopyInfo = PObject(
      "id" -> datasetInfoVar,
      "sid" -> systemIdVar,
      "num" -> copyNumberVar,
      "stage" -> lifecycleStageVar,
      "ver" -> dataVersionVar
    )

    def encode(x: CopyInfo) = PCopyInfo.generate(
      datasetInfoVar := x.datasetInfo,
      systemIdVar := x.systemId,
      copyNumberVar := x.copyNumber,
      lifecycleStageVar := x.lifecycleStage,
      dataVersionVar := x.dataVersion
    )

    def decode(x: JValue) = PCopyInfo.matches(x) map { results =>
      SimpleCopyInfo(
        datasetInfo = datasetInfoVar(results),
        systemId = systemIdVar(results),
        copyNumber = copyNumberVar(results),
        lifecycleStage = lifecycleStageVar(results),
        dataVersion = dataVersionVar(results)
      )
    }
  }
}
