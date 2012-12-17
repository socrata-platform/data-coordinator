package com.socrata.datacoordinator
package truth.metadata

import scala.runtime.ScalaRunTime

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{PObject, Variable}
import com.rojoma.json.ast.JValue

trait VersionInfo {
  def datasetInfo: DatasetInfo
  def systemId: VersionId
  def lifecycleVersion: Long
  def lifecycleStage: LifecycleStage

  final override def hashCode = ScalaRunTime._hashCode((datasetInfo, systemId, lifecycleVersion, lifecycleStage))
  final override def equals(o: Any) = o match {
    case that: VersionInfo =>
      (this eq that) ||
        (this.datasetInfo == that.datasetInfo && this.systemId == that.systemId && this.lifecycleVersion == that.lifecycleVersion && this.lifecycleStage == that.lifecycleStage)
    case _ =>
      false
  }
}

object VersionInfo {
  implicit val jCodec = new JsonCodec[VersionInfo] {
    implicit def lifecycleCodec = LifecycleStage.jCodec

    val datasetInfoV = Variable[DatasetInfo]
    val systemIdV = Variable[VersionId]
    val lifecycleVersionV = Variable[Long]
    val lifecycleStageV = Variable[LifecycleStage]

    val Pattern = new PObject(
      "datasetInfo" -> datasetInfoV,
      "systemId" -> systemIdV,
      "lifecycleVersion" -> lifecycleVersionV,
      "lifecycleStage" -> lifecycleStageV
    )

    def encode(vi: VersionInfo): JValue =
      Pattern.generate(
        datasetInfoV := vi.datasetInfo,
        systemIdV := vi.systemId,
        lifecycleVersionV := vi.lifecycleVersion,
        lifecycleStageV := vi.lifecycleStage
      )

    def decode(x: JValue) = Pattern.matches(x) map { res =>
      new VersionInfo {
        val datasetInfo = datasetInfoV(res)
        val systemId = systemIdV(res)
        val lifecycleVersion = lifecycleVersionV(res)
        val lifecycleStage = lifecycleStageV(res)
      }
    }
  }
}
