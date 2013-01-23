package com.socrata.datacoordinator
package truth.metadata

import com.socrata.datacoordinator.id.VersionId
import com.rojoma.json.util.AutomaticJsonCodecBuilder
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{PObject, Variable}
import com.rojoma.json.ast.JValue
import scala.runtime.ScalaRunTime

trait VersionInfo extends Product {
  val datasetInfo: DatasetInfo
  val systemId: VersionId
  val lifecycleVersion: Long
  val lifecycleStage: LifecycleStage

  // The systemid is needed to prevent clashes in the following situation:
  //   Working copy created
  //   Working copy dropped (note: this enqueues the table for later dropping)
  //   Working copy created
  // If only the lifecycle version were used, the second working copy creation
  // would try to make a table with the same name as the first, which at that
  // point still exists.
  lazy val dataTableName = datasetInfo.tableBase + "_" + systemId.underlying + "_" + lifecycleVersion

  override final def hashCode = ScalaRunTime._hashCode(this)
  override final def productPrefix = "VersionInfo"
  override final def toString = ScalaRunTime._toString(this)
  override final def equals(o: Any) = o match {
    case that: VersionInfo =>
      ScalaRunTime._equals(this, that)
    case _ =>
      false
  }
}

case class SimpleVersionInfo(datasetInfo: DatasetInfo, systemId: VersionId, lifecycleVersion: Long, lifecycleStage: LifecycleStage) extends VersionInfo

object VersionInfo {
  implicit object jCodec extends JsonCodec[VersionInfo] {
    private implicit def lifecycleCodec = LifecycleStage.jCodec

    val datasetInfoVar = Variable[DatasetInfo]
    val systemIdVar = Variable[VersionId]
    val lifecycleVersionVar = Variable[Long]
    val lifecycleStageVar = Variable[LifecycleStage]

    val PVersionInfo = PObject(
      "di" -> datasetInfoVar,
      "sid" -> systemIdVar,
      "ver" -> lifecycleVersionVar,
      "stage" -> lifecycleStageVar
    )

    def encode(x: VersionInfo) = PVersionInfo.generate(
      datasetInfoVar := x.datasetInfo,
      systemIdVar := x.systemId,
      lifecycleVersionVar := x.lifecycleVersion,
      lifecycleStageVar := x.lifecycleStage
    )

    def decode(x: JValue) = PVersionInfo.matches(x) map { results =>
      SimpleVersionInfo(
        datasetInfo = datasetInfoVar(results),
        systemId = systemIdVar(results),
        lifecycleVersion = lifecycleVersionVar(results),
        lifecycleStage = lifecycleStageVar(results)
      )
    }
  }
}
