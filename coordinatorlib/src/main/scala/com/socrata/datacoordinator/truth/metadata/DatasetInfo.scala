package com.socrata.datacoordinator
package truth.metadata

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{PObject, Variable}
import com.rojoma.json.ast.JValue

trait DatasetInfo {
  def systemId: DatasetId
  def datasetId: String
  def tableBase: String
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
