package com.socrata.datacoordinator
package truth.metadata

import scala.runtime.ScalaRunTime

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{PObject, Variable}
import com.rojoma.json.ast.JValue
import com.socrata.datacoordinator.id.DatasetId

trait DatasetInfo {
  def systemId: DatasetId
  def datasetId: String
  def tableBase: String

  lazy val logTableName = tableBase + "_" + systemId.underlying + "_log"

  final override def hashCode = ScalaRunTime._hashCode((systemId, datasetId, tableBase))
  final override def equals(o: Any) = o match {
    case that: DatasetInfo =>
      (this eq that) ||
        (this.systemId == that.systemId && this.datasetId == that.datasetId && this.tableBase == that.tableBase)
    case _ =>
      false
  }
}

object DatasetInfo {
  implicit val jCodec = new JsonCodec[DatasetInfo] {
    val systemIdV = Variable[DatasetId]
    val datasetIdV = Variable[String]
    val tableBaseV = Variable[String]

    val Pattern = new PObject(
      "systemId" -> systemIdV,
      "datasetId" -> datasetIdV,
      "tableBase" -> tableBaseV
    )

    def encode(di: DatasetInfo): JValue =
      Pattern.generate(
        systemIdV := di.systemId,
        datasetIdV := di.datasetId,
        tableBaseV := di.tableBase
      )

    def decode(x: JValue) = Pattern.matches(x) map { res =>
      new DatasetInfo {
        val systemId = systemIdV(res)
        val datasetId = datasetIdV(res)
        val tableBase = tableBaseV(res)
      }
    }
  }
}
