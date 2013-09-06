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
      getSchema(schema, dsInfo.localeName)
    }

  def schemaHash(schema: ColumnIdMap[ColumnInfo[CT]], locale: String) =
    SchemaHash.computeHash(schema, locale, typeSerializer)

  def getSchema(schema: ColumnIdMap[ColumnInfo[CT]], locale: String): Schema = {
    val hash = schemaHash(schema, locale)
    val columns = RotateSchema(schema).mapValuesStrict { col => typeSerializer(col.typ) }
    val pk = schema.values.find(_.isUserPrimaryKey).orElse(schema.values.find(_.isSystemPrimaryKey)).getOrElse {
      sys.error("No system primary key column?")
    }.userColumnId
    Schema(hash, columns, pk, locale)
  }
}
