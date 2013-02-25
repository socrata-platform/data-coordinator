package com.socrata.datacoordinator.service

import scalaz._
import scalaz.effect._
import Scalaz._

import java.io.{FileNotFoundException, InputStream}

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.DataWritingContext
import com.socrata.datacoordinator.primary.DatasetCreator
import scala.collection.mutable
import com.socrata.csv.CSVIterator

class BadSchemaException(msg: String) extends Exception(msg)

class FileImporter(openFile: String => InputStream,
                   val dataContext: DataWritingContext) {
  import dataContext.datasetMutator._

  private def analyseSchema(in: Seq[Field]) = {
    val result = new mutable.LinkedHashMap[String, dataContext.CT] // want to preserve the order
    for(field <- in) {
      val name = field.name.toLowerCase
      if(result.contains(name)) throw new BadSchemaException("Field " + field.name + " defined more than once")
      if(!dataContext.isLegalLogicalName(name)) throw new BadSchemaException("Field " + field + " has an invalid name")
      val typ = dataContext.typeContext.typeFromNameOpt(field.typ).getOrElse { throw new BadSchemaException("Unknown type " + field.typ) }
      result += name -> typ
    }
    result
  }

  private def decodeRow(row: IndexedSeq[String]): dataContext.Row = {
    ???
  }

  /**
   * @throws FileNotFoundException if `fileId` does not name a known file
   * @throws DatasetAlreadyExistsException if `id` names an extant dataset
   * @throws BadSchemaException if schema names the same field twice, or if a type is unknown
   */
  def importFile(id: String, fileId: String, rawSchema: Seq[Field]) {
    val cookedSchema = analyseSchema(rawSchema)
    using(openFile(fileId)) { stream =>
      creatingDataset(as = "unknown")(id, "t") {
        for {
          _ <- dataContext.addSystemColumns
          _ <- cookedSchema.toList.map { case (name, typ) =>
            addColumn(name, dataContext.typeContext.nameFromType(typ), dataContext.physicalColumnBaseBase(name).toLowerCase)
          }.sequenceU.map(_ => ())
          s <- schema
          report <- upsert(IO(CSVIterator.fromInputStream(stream).map(decodeRow).map(Right(_))))
        } yield report
      }
    }
  }
}
