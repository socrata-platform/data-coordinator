package com.socrata.datacoordinator.service

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.universe.{DatasetMapReaderProvider, Universe}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.util.RotateSchema
import com.socrata.soql.environment.TypeName
import com.socrata.datacoordinator.id.DatasetId

class SchemaFinder[CT, CV](universe: Managed[Universe[CT, CV] with DatasetMapReaderProvider], typeSerializer: CT => TypeName) {
  def getSchema(datasetId: DatasetId): Option[Schema] =
    for {
      u <- universe
      dsInfo <- u.datasetMapReader.datasetInfo(datasetId)
    } yield {
      val schema = u.datasetMapReader.schema(u.datasetMapReader.latest(dsInfo))
      getSchema(schema)
    }

  def schemaHash(schema: ColumnIdMap[ColumnInfo[CT]]) =
    SchemaHash.computeHash(schema, typeSerializer)

  def getSchema(schema: ColumnIdMap[ColumnInfo[CT]]): Schema = {
    val hash = schemaHash(schema)
    Schema(hash, RotateSchema(schema).mapValues { col => typeSerializer(col.typ) }, schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
      sys.error("No system primary key column?")
    }.logicalName)
  }
}
