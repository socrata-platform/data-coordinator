package com.socrata.datacoordinator.service

import com.rojoma.json.ast.JObject

import com.socrata.datacoordinator.truth.{JsonDataWritingContext, DataReadingContext}

class Exporter(val dataContext: DataReadingContext with JsonDataWritingContext) {
  def export(id: String)(f: Iterator[JObject] => Unit): Boolean = {
    dataContext.withRows(id) { it =>
      val schema = dataContext.jsonSchema(id).getOrElse(sys.error("The dataset was JUST THERE!"))
      f(it.map(dataContext.toJObject(schema, _)))
    }.isDefined
  }
}
