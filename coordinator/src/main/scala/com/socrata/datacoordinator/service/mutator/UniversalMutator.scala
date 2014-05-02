package com.socrata.datacoordinator.service.mutator

import com.rojoma.json.ast.JValue
import com.socrata.datacoordinator.id.DatasetId
import org.joda.time.DateTime
import com.rojoma.simplearm.{SimpleArm, Managed}
import com.rojoma.json.io.JsonReader
import java.nio.charset.StandardCharsets

trait UniversalMutator {
  def createScript(commandStream: Iterator[JValue]): (DatasetId, Long, DateTime, Seq[MutationScriptCommandResult])
  def copyOp(datasetId: DatasetId, command: JValue): (Long, DateTime)
  def ddlScript(udatasetId: DatasetId, commandStream: Iterator[JValue]): (Long, DateTime, Seq[MutationScriptCommandResult])

  def upsertScript(datasetId: DatasetId, commandStream: Iterator[JValue]): Managed[(Long, DateTime, Iterator[JValue])] =
    new SimpleArm[(Long, DateTime, Iterator[JValue])] {
      override def flatMap[A](f: ((Long, DateTime, Iterator[JValue])) => A): A = {
        upsertJson(datasetId, commandStream).flatMap { case (v, dt, json) =>
          val jvalues = json.map(JsonReader.fromString)
          f((v, dt, jvalues))
        }
      }
    }

  // same as upsertScript, but returns its results as pre-serialized strings.
  def upsertJson(datasetId: DatasetId, commandStream: Iterator[JValue]): Managed[(Long, DateTime, Iterator[String])] =
    new SimpleArm[(Long, DateTime, Iterator[String])] {
      override def flatMap[A](f: ((Long, DateTime, Iterator[String])) => A): A = {
        upsertUtf8(datasetId, commandStream).flatMap { case (v, dt, json) =>
          val strings = json.map(new String(_, StandardCharsets.UTF_8))
          f((v, dt, strings))
        }
      }
    }

  // same as upsertScript, but returns its results as pre-serialized UTF-8 bytestrings.
  def upsertUtf8(datasetId: DatasetId, commandStream: Iterator[JValue]): Managed[(Long, DateTime, Iterator[Array[Byte]])]
}
