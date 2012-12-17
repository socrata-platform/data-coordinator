package com.socrata.datacoordinator
package truth.metadata

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{PObject, Variable}
import com.rojoma.json.ast.JValue

trait VersionInfo {
  def datasetInfo: DatasetInfo
  def systemId: VersionId
  def lifecycleVersion: Long
  def lifecycleStage: LifecycleStage
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
