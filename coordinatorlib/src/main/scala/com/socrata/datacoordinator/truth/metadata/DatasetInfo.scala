package com.socrata.datacoordinator
package truth.metadata

import scala.runtime.ScalaRunTime

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{Variable, PObject}
import com.rojoma.json.ast.JValue

import com.socrata.datacoordinator.id.{RowId, DatasetId}

trait DatasetInfo extends Product {
  val systemId: DatasetId
  val datasetId: String
  val tableBaseBase: String
  val nextRowId: RowId

  lazy val tableBase = tableBaseBase + "_" + systemId.underlying
  lazy val logTableName = tableBase + "_log"

  override final def hashCode = ScalaRunTime._hashCode(this)
  override final def productPrefix = "DatasetInfo"
  override final def toString = ScalaRunTime._toString(this)
  override final def equals(o: Any) = o match {
    case that: DatasetInfo =>
      ScalaRunTime._equals(this, that)
    case _ =>
      false
  }
}

case class SimpleDatasetInfo(systemId: DatasetId, datasetId: String, tableBaseBase: String, nextRowId: RowId) extends DatasetInfo

object DatasetInfo {
  implicit object jCodec extends JsonCodec[DatasetInfo] {
    val systemIdVar = Variable[DatasetId]
    val datasetIdVar = Variable[String]
    val tableBaseBaseVar = Variable[String]
    val nextRowIdVar = Variable[RowId]

    val PDatasetInfo = PObject(
      "sid" -> systemIdVar,
      "id" -> datasetIdVar,
      "tbase" -> tableBaseBaseVar,
      "rid" -> nextRowIdVar
    )

    def encode(x: DatasetInfo) = PDatasetInfo.generate(
      systemIdVar := x.systemId,
      datasetIdVar := x.datasetId,
      tableBaseBaseVar := x.tableBaseBase,
      nextRowIdVar := x.nextRowId
    )

    def decode(x: JValue) = PDatasetInfo.matches(x) map { results =>
      SimpleDatasetInfo(
        systemId = systemIdVar(results),
        datasetId = datasetIdVar(results),
        tableBaseBase = tableBaseBaseVar(results),
        nextRowId = nextRowIdVar(results)
      )
    }
  }
}
