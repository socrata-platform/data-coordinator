package com.socrata.datacoordinator.service

import scalaz._
import scalaz.effect._
import Scalaz._

import java.io.{FileNotFoundException, InputStream}

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.{CsvDataContext, DataWritingContext}
import com.socrata.datacoordinator.primary.DatasetCreator
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import scala.collection.mutable
import com.socrata.csv.CSVIterator
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class BadSchemaException(msg: String) extends Exception(msg)

class FileImporter(openFile: String => InputStream,
                   val dataContext: DataWritingContext with CsvDataContext) {
  import dataContext.datasetMutator._

  import dataContext.{CT, CV, Row, MutableRow}

  private def analyseSchema(in: Seq[Field]) = {
    val result = new mutable.LinkedHashMap[String, CT] // want to preserve the order
    for(field <- in) {
      val name = field.name.toLowerCase
      if(result.contains(name)) throw new BadSchemaException("Field " + field.name + " defined more than once")
      if(!dataContext.isLegalLogicalName(name)) throw new BadSchemaException("Field " + field + " has an invalid name")
      val typ = dataContext.typeContext.typeFromNameOpt(field.typ).getOrElse { throw new BadSchemaException("Unknown type " + field.typ) }
      result += name -> typ
    }
    result
  }

  private def rowDecodePlan(fieldNames: mutable.LinkedHashMap[String, CT], schema: Map[String, ColumnInfo]): IndexedSeq[String] => (Seq[String], Row) = {
    val colInfo = fieldNames.iterator.zipWithIndex.map { case ((field, typ), idx) =>
      val ci = schema(field)
      (field, ci.systemId, dataContext.csvRepForColumn(typ), Array(idx) : IndexedSeq[Int])
    }.toList
    (row: IndexedSeq[String]) => {
      val result = new MutableRow
      val bads = colInfo.flatMap { case (header, systemId, rep, indices) =>
        try {
          result += systemId -> rep.decode(row, indices).get
          Nil
        } catch { case e: Exception => List(header) }
      }
      (bads, result.freeze())
    }
  }

  /**
   * @throws FileNotFoundException if `fileId` does not name a known file
   * @throws DatasetAlreadyExistsException if `id` names an extant dataset
   * @throws BadSchemaException if schema names the same field twice, or if a type is unknown
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
    using(openFile(fileId)) { stream =>
      creatingDataset(as = "unknown")(id, "t") {
        for {
          _ <- dataContext.addSystemColumns
          _ <- cookedSchema.toList.map { case (name, typ) =>
            addColumn(name, dataContext.typeContext.nameFromType(typ), dataContext.physicalColumnBaseBase(name).toLowerCase).flatMap(primaryKeyXForm)
          }.sequenceU.map(_ => ())
          plan <- schemaByLogicalName.map(rowDecodePlan(cookedSchema, _))
          report <- upsert(IO(CSVIterator.fromInputStream(stream).map(plan).map { r => Right(r._2) }))
        } yield report
      }.unsafePerformIO()
    }
  }
}
