package com.socrata.datacoordinator.service

import scala.{collection => sc}
import scala.io.Codec

import java.io.{FileNotFoundException, InputStream, BufferedReader, InputStreamReader}
import java.util.zip.GZIPInputStream

import com.rojoma.simplearm.util._
import com.rojoma.json.ast._
import com.rojoma.json.io.{JsonEventIterator, JsonReaderException}
import com.rojoma.json.util.JsonArrayIterator
import com.socrata.datacoordinator.truth.{JsonDataContext, JsonDataReadingContext, DataWritingContext}
import com.socrata.datacoordinator.truth.json.JsonColumnReadRep
import com.socrata.datacoordinator.primary.DatasetCreator
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.loader.NoSuchRowToDelete
import com.socrata.datacoordinator.truth.loader.NoSuchRowToUpdate
import com.socrata.datacoordinator.truth.loader.SystemColumnsSet

class BadSchemaException(msg: String) extends Exception(msg)
class BadDataException(msg: String, cause: Throwable = null) extends Exception(msg, cause)

// TODO: This needs to normalize combining characters!
class FileImporter(openFile: String => InputStream,
                   magicDeleteKey: String, // should this come from the context?
                   val dataContext: DataWritingContext with JsonDataContext) {
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

  private def rowDecodePlan(schema: ColumnIdMap[ColumnInfo]): JObject => Either[CV, Row] = {
    val pkCol = schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
      sys.error("No system primary key in the schema?")
    }
    val pkId = pkCol.logicalName
    val pkRep = dataContext.jsonRepForColumn(pkCol)
    val cookedSchema: Map[String, (ColumnId, JsonColumnReadRep[CT, CV])] =
      schema.iterator.map { case (systemId, ci) =>
        ci.logicalName -> (systemId, dataContext.jsonRepForColumn(ci))
      }.toMap
    (row: JObject) => {
      if(row.contains(magicDeleteKey) && row(magicDeleteKey) == JBoolean.canonicalTrue) {
        row.get(pkId) match {
          case Some(value) =>
            pkRep.fromJValue(value) match {
              case Some(trueValue) =>
                Left(trueValue)
              case None =>
                throw new BadDataException("Unable to interpret field " + pkId)
            }
          case None =>
            throw new BadDataException("Delete command without associated ID value")
        }
      } else {
        val result = new MutableRow
        cookedSchema.iterator.foreach { case (field, (systemId, rep)) =>
          row.get(field) match {
            case Some(value) =>
              rep.fromJValue(value) match {
                case Some(trueValue) =>
                  result += systemId -> trueValue
                case None =>
                  throw new BadDataException("Unable to interpret field " + field)
              }
            case None =>
              /* pass */
          }
        }
        Right(result.freeze())
      }
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
        (ci: ColumnInfo, ctx: dataContext.datasetMutator.MutationContext) =>
          if(ci.logicalName == pk) ctx.makeUserPrimaryKey(ci)
          else ci
      case None =>
        (ci: ColumnInfo, ctx: dataContext.datasetMutator.MutationContext) => ci
    }
    try {
      for {
        f <- managed(openFile(fileId))
        // stream <- managed(new GZIPInputStream(f))
        reader <- managed(new BufferedReader(new InputStreamReader(f, Codec.UTF8.charSet)))
      } yield {
        for(ctx <- createDataset(as = "unknown")(id, "t")) {
          import ctx._
          dataContext.addSystemColumns(ctx)
          cookedSchema.toList.foreach { case (name, typ) =>
            primaryKeyXForm(addColumn(name, dataContext.typeContext.nameFromType(typ), dataContext.physicalColumnBaseBase(name).toLowerCase), ctx)
          }
          val plan = rowDecodePlan(schema)
          upsert {
            val rowIterator = JsonArrayIterator[JObject](new JsonEventIterator(reader))
            rowIterator.map(plan)
          }
        }
      }
    } catch {
      case e: JsonReaderException =>
        throw new BadDataException(s"Unable to parse data as JSON at ${e.position.row}:${e.position.column}", e)
      case e: JsonArrayIterator.ElementDecodeException =>
        throw new BadDataException(s"An element was not a JSON object at ${e.position.row}:${e.position.column}", e)
    }
  }

  def updateFile(id: String, inputStream: InputStream): Option[JObject] = { // TODO: Real report
    try {
      for {
        // stream <- managed(new GZIPInputStream(inputStream))
        reader <- managed(new BufferedReader(new InputStreamReader(inputStream, Codec.UTF8.charSet)))
      } yield {
        for {
          ctxOpt <- openDataset(as = "unknown")(id)
          ctx <- ctxOpt
        } yield {
          import ctx._
          val report = upsert {
            val rowIterator = JsonArrayIterator[JObject](new JsonEventIterator(reader))
            rowIterator.map(rowDecodePlan(schema))
          }
          val idEncoder = dataContext.jsonRepForColumn(schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
            sys.error("No system PK defined?")
          })
          val inserted = report.inserted.valuesIterator.map(idEncoder.toJValue).toStream
          val updated = report.updated.valuesIterator.map(idEncoder.toJValue).toStream
          val deleted = report.deleted.valuesIterator.map(idEncoder.toJValue).toStream
          val errors = report.errors.valuesIterator.map {
            case NullPrimaryKey => JString("NullPrimaryKey")
            case SystemColumnsSet(names: ColumnIdSet) => JObject(Map("System columns set" -> JArray(names.iterator.map { cid => JString(schema(cid).logicalName) }.toSeq)))
            case NoSuchRowToDelete(id) => JObject(Map("NoSuchRowToDelete" -> idEncoder.toJValue(id)))
            case NoSuchRowToUpdate(id) => JObject(Map("NoSuchRowToUpdate" -> idEncoder.toJValue(id)))
            case NoPrimaryKey => JString("NoPrimaryKey")
          }.toStream
          JObject(Map(
            "inserted" -> JArray(inserted),
            "updated" -> JArray(updated),
            "deleted" -> JArray(deleted),
            "errors" -> JArray(errors)
          ))
        }
      }
    } catch {
      case e: JsonReaderException =>
        throw new BadDataException(s"Unable to parse data as JSON at ${e.position.row}:${e.position.column}", e)
      case e: JsonArrayIterator.ElementDecodeException =>
        throw new BadDataException(s"An element was not a JSON object at ${e.position.row}:${e.position.column}", e)
    }
  }
}
