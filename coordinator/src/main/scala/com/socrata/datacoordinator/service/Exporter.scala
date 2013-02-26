package com.socrata.datacoordinator.service

import scalaz.effect._
import com.rojoma.json.ast.{JValue, JObject}

import com.socrata.datacoordinator.truth.{JsonDataWritingContext, DataReadingContext}

class Exporter(val dataContext: DataReadingContext with JsonDataWritingContext) {
  def export(id: String)(f: Iterator[JObject] => Unit): Boolean = {
    val res = dataContext.datasetReader.withDataset(id, latest = true) {
      import dataContext.datasetReader._
      for {
        s <- schema
        jsonSchema = s.mapValuesStrict(dataContext.jsonRepForColumn)
        _ <- dataContext.datasetReader.withRows { it =>
          IO {
            val objectified = it.map { row =>
              val res = new scala.collection.mutable.HashMap[String, JValue]
              row.foreach { case (cid, value) =>
                val rep = jsonSchema(cid)
                res(rep.name) = rep.toJValue(value)
              }
              JObject(res)
            }
            f(objectified)
          }
        }
      } yield ()
    }

    res.unsafePerformIO().isDefined
  }
}
