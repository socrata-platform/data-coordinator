package com.socrata.datacoordinator.service

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.universe.{DatasetMapReaderProvider, Universe}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import scala.collection.mutable
import com.rojoma.json.ast.{JString, JObject, JValue}
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class SchemaFinder[CT, CV](universe: Managed[Universe[CT, CV] with DatasetMapReaderProvider], typeSerializer: CT => String) {
  private def jsonify(schema: ColumnIdMap[ColumnInfo[CT]]): JObject = {
    val m = new mutable.HashMap[String, JValue]
    for(c <- schema.values) {
      m(c.logicalName.name) = JString(typeSerializer(c.typ))
    }
    JObject(m)
  }

  def getSchema(datasetName: String): Option[Schema] = {
    for {
      u <- universe
      dsid <- u.datasetMapReader.datasetId(datasetName)
      dsInfo <- u.datasetMapReader.datasetInfo(dsid)
    } yield {
      val schema = u.datasetMapReader.schema(u.datasetMapReader.latest(dsInfo))
      val schemaHash = SchemaHash.computeHash(schema, typeSerializer)
      Schema(schemaHash, jsonify(schema), schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
        sys.error("No system primary key column?")
      }.logicalName)
    }
  }
}
