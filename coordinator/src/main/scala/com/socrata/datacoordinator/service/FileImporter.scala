package com.socrata.datacoordinator.service

import scala.{collection => sc}
import scala.io.Codec

import java.io.{FileNotFoundException, InputStream, BufferedReader, InputStreamReader}
import java.util.zip.GZIPInputStream

import scalaz._
import scalaz.effect._
import Scalaz._

import com.rojoma.simplearm.util._
import com.rojoma.json.ast._
import com.rojoma.json.io.{JsonEventIterator, JsonReaderException}
import com.rojoma.json.util.JsonArrayIterator
import com.socrata.datacoordinator.truth.{JsonDataReadingContext, DataWritingContext}
import com.socrata.datacoordinator.truth.json.JsonColumnReadRep
import com.socrata.datacoordinator.primary.DatasetCreator
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class BadSchemaException(msg: String) extends Exception(msg)
class BadDataException(msg: String, cause: Throwable = null) extends Exception(msg, cause)

class FileImporter(openFile: String => InputStream,
                   val dataContext: DataWritingContext with JsonDataReadingContext) {
  import dataContext.datasetMutator._

  import dataContext.{CT, CV, Row, MutableRow}

  private def analyseSchema(in: Seq[Field]) = {
    in.foldLeft(Map.empty[String, CT]) { (acc, field) =>
      val name = field.name.toLowerCase
      if(acc.contains(name)) throw new BadSchemaException("Field " + field.name + " defined more than once")
      if(!dataContext.isLegalLogicalName(name)) throw new BadSchemaException("Field " + field + " has an invalid name")
      val typ = dataContext.typeContext.typeFromNameOpt(field.typ).getOrElse { throw new BadSchemaException("Unknown type " + field.typ) }
      acc + (name -> typ)
    }
  }

  private def rowDecodePlan(schema: ColumnIdMap[ColumnInfo]): JObject => (Seq[String], Row) = {
    val cookedSchema: Map[String, (ColumnId, JsonColumnReadRep[CT, CV])] =
      schema.iterator.map { case (systemId, ci) =>
        ci.logicalName -> (systemId, dataContext.jsonRepForColumn(ci))
      }.toMap
    (row: JObject) => {
      val result = new MutableRow
      val bads = cookedSchema.iterator.flatMap { case (field, (systemId, rep)) =>
        row.get(field) match {
          case Some(value) =>
            rep.fromJValue(value) match {
              case Some(trueValue) =>
                result += systemId -> trueValue
                Nil
              case None =>
                List(field)
            }
          case None =>
            Nil
        }
      }
      (bads.toList, result.freeze())
    }
  }

  /**
   * @throws FileNotFoundException if `fileId` does not name a known file
   * @throws DatasetAlreadyExistsException if `id` names an extant dataset
   * @throws BadSchemaException if schema names the same field twice, or if a type is unknown
   * @throws BadDataException if a JSON parse exception occurs, or if the data is not an array
   *                          of objects, or if an object contains fields that cannot be decoded.
   */
  def importFile(id: String, fileId: String, rawSchema: Seq[Field], primaryKey: Option[String]) {
    val cookedSchema = analyseSchema(rawSchema)
    val primaryKeyXForm = primaryKey match {
      case Some(pk) =>
        (ci: ColumnInfo) =>
          if(ci.logicalName == pk) makeUserPrimaryKey(ci)
          else ci.pure[DatasetM]
      case None =>
        (ci: ColumnInfo) => ci.pure[DatasetM]
    }
    try {
      for {
        f <- managed(openFile(fileId))
        stream <- managed(new GZIPInputStream(f))
        reader <- managed(new BufferedReader(new InputStreamReader(stream, Codec.UTF8.charSet)))
      } yield {
        creatingDataset(as = "unknown")(id, "t") {
          for {
            _ <- dataContext.addSystemColumns
            _ <- cookedSchema.toList.map { case (name, typ) =>
              addColumn(name, dataContext.typeContext.nameFromType(typ), dataContext.physicalColumnBaseBase(name).toLowerCase).flatMap(primaryKeyXForm)
            }.sequenceU.map(_ => ())
            plan <- schema.map(rowDecodePlan)
            report <- upsert(IO {
              val rowIterator = JsonArrayIterator[JObject](new JsonEventIterator(reader))
              rowIterator.map(plan).map { r =>
                if(r._1.nonEmpty) throw new BadDataException("Unable to interpret fields " + r._1.mkString(", "))
                Right(r._2)
              }
            })
          } yield report
        }.unsafePerformIO()
      }
    } catch {
      case e: JsonReaderException =>
        throw new BadDataException(s"Unable to parse data as JSON at ${e.position.row}:${e.position.column}", e)
      case e: JsonArrayIterator.ElementDecodeException =>
        throw new BadDataException(s"An element was not a JSON object at ${e.position.row}:${e.position.column}", e)
    }
  }
}
